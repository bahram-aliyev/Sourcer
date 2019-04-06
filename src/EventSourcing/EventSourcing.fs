namespace Sourcer

module FSharp =

    module Extras =

        [<RequireQualifiedAccess>]
        module Async =

            let retn x = async.Return x
    
            let bind f xA = 
                (xA, f) |> async.Bind
    
            let map f xA = async { 
                let! x = xA
                return f x 
                }

            let apply fA xA = async { 
                let! fAc = Async.StartChild fA
                let! x = xA
                let! f = fAc
                return f x 
                }


        type AsyncResult<'a, 'b> = Async<Result<'a, 'b>>

        [<RequireQualifiedAccess>]
        module AsyncResult =

            let map f (x : AsyncResult<_,_>) : AsyncResult<_,_> =
                x |> Async.map (Result.map f)

            let mapError f (x : AsyncResult<_,_>) : AsyncResult<_,_> =
                x |> Async.map (Result.mapError f)

            let retn x : AsyncResult<_,_> = 
                x |> (Result.Ok >> Async.retn)

            let ofResult xR : AsyncResult<_,_> = 
                xR |> Async.retn

            let bind (f: 'a -> AsyncResult<'b,'c>) (xAr : AsyncResult<_, _>) :AsyncResult<_,_> = async {
                let! xR = xAr 
                match xR with
                | Ok x -> return! f x
                | Error err -> return (Error err)
                }

            let catch f (x : AsyncResult<_,_>) : AsyncResult<_,_> =
                x
                |> Async.Catch
                |> Async.map(function
                    | Choice1Of2 (Ok v) -> Ok v
                    | Choice1Of2 (Error err) -> Error err
                    | Choice2Of2 ex -> Error (f ex))

    
module EventSourcing =

    open System
    open FSharp.Extras


    type AggregateVersion = internal AggregateVersion of int
    module AggregateVersion =

        let create x =
            if x > 0
            then Ok <| AggregateVersion x
            else Error <| sprintf "Aggregate version must be a positive integer value, current: %i" x            

        let value (AggregateVersion x) = x

        let internal inc (AggregateVersion x) = AggregateVersion(x + 1)

        let zero = AggregateVersion(0)


    type ErrorMessage = string


    type Aggregate<'state, 'action, 'event> = 
            Aggregate of zero : 'state 
                        * apply : ('state -> 'event -> 'state) 
                        * exec : ('state -> 'action -> Result<'event list, ErrorMessage list>)


    type AggregateCommand<'aid, 'action> =
        { AggregateId : 'aid
          AggregateAction : 'action
          AggregateVersion : AggregateVersion }


    type AggregateCommandFailure =
        | ConcurrencyError of ErrorMessage
        | ValidationError of ErrorMessage list
        | VoidStateChangeError of ErrorMessage
        | PersistenceException of exn


    type LoadStateFailure =
        | LoadException of exn
        | StateNotExist of aggregateId : string * version : int * eventType : Type
        | NotFound of aggregateId : string * eventType : Type


    type CommitEventsFailure =
        | DuplicateVersionError of ErrorMessage
        | PersisitenceException of exn


    type LoadEventsFailure = 
        | LoadException of exn


    type EventStore<'aid, 'event> = 
            EventStore of commit : (('aid * ('event * AggregateVersion) list) -> AsyncResult<unit, CommitEventsFailure>)
                        * load : ('aid -> AsyncResult<'event list, LoadEventsFailure>)


    type EventStream<'aid, 'event> =
            EventStream of load : ('aid * AggregateVersion option -> AsyncResult<'event list, LoadEventsFailure>)


    [<RequireQualifiedAccess>]
    module Aggregate =


        type private VersionedEvent<'event> =  'event * AggregateVersion


        let restoreState 
            (apply : 'state -> 'event -> 'state) 
            (state : 'state * AggregateVersion)
            (events : 'event list) =
            (state, events) ||> List.fold (fun (s, v) e -> apply s e, AggregateVersion.inc v)


        let handleCommand
            (Aggregate (zero, apply, exec) : Aggregate<'state, 'action, 'event>)
            (EventStore (commit, load) : EventStore<'aid, 'event>)
            (cmd : AggregateCommand<'aid, 'action>) =

            let loadEvents =
                load >> AsyncResult.mapError (
                    fun (LoadException ex) -> PersistenceException ex)

            let execCmd { AggregateId = aid; AggregateVersion = av; AggregateAction = act } en =

                let ensureVersion av en =
                    let crnt = en |> (List.length >> AggregateVersion)
                    match av = crnt with
                    | true -> Ok en
                    | _ ->
                        (AggregateVersion.value av, AggregateVersion.value crnt)
                        ||> sprintf "Concurrency error, actual aggregate version '%i' does not match expected '%i'."
                        |> (ConcurrencyError >> Error)

                let applyAction act (state, av) =
                    let rec versionize (ven : VersionedEvent<_> list) v en =
                        match en with
                        | h::t -> 
                            let v = AggregateVersion.inc v 
                            versionize ((h, v) :: ven) v t
                        | [] -> List.rev ven

                    (state, act)
                    ||> exec 
                    |> Result.mapError ValidationError
                    |> Result.map (versionize [] av)

                let esureEvents aid en =
                    match en with
                    | [] ->
                        sprintf "Command produced no events of type '%s' for aggregateId:%s." typeof<'event>.Name (aid.ToString())
                        |> (VoidStateChangeError >> Error)
                    | _ -> Result.Ok en

                (av, en)
                ||> ensureVersion 
                |> Result.map (restoreState apply (zero, AggregateVersion.zero))
                |> Result.bind (applyAction act)
                |> Result.bind (esureEvents aid)
                |> AsyncResult.ofResult

            let commitEvents aid (ven : VersionedEvent<_> list) =
                (aid, ven)
                |> commit 
                |> AsyncResult.mapError (function
                    | DuplicateVersionError e -> AggregateCommandFailure.ConcurrencyError e
                    | PersisitenceException ex -> AggregateCommandFailure.PersistenceException ex)


            cmd.AggregateId
            |> loadEvents
            |> AsyncResult.bind (execCmd cmd)
            |> AsyncResult.bind (commitEvents cmd.AggregateId)


        let loadState
            (Aggregate (zero, apply, _) : Aggregate<'state, 'action, 'event>)
            (EventStream (load) : EventStream<'aid, 'event>) 
            (id : 'aid, av : AggregateVersion option) =

            let loadEvents =
                load >> AsyncResult.mapError (
                    fun (LoadException ex) -> LoadStateFailure.LoadException ex)

            let verifyStream = 
                function
                | [] -> NotFound ((id.ToString()), typeof<'event>) |> Error
                | en -> 
                    match av with
                    | Some v -> 
                        let sv = AggregateVersion <| List.length en
                        match sv = v with
                        | true -> Ok en
                        | _ -> StateNotExist (id.ToString(), AggregateVersion.value v, typeof<'event>) |> Error
                    | _ -> Ok en

            let restoreState = 
                restoreState apply (zero, AggregateVersion.zero)

            (id, av)
            |> loadEvents 
            |> Async.map (
                Result.bind verifyStream 
                >> Result.map restoreState)