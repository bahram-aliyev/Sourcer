namespace Sourcer.EventSourcing

[<RequireQualifiedAccess>]
module Aggregate =

    open Sourcer.FSharp.Extras


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

        let ensureVersion av en =
            let crnt = en |> (List.length >> AggregateVersion)
            match av = crnt with
            | true -> Ok en
            | _ ->
                (AggregateVersion.value av, AggregateVersion.value crnt)
                ||> sprintf "Concurrency error, actual aggregate version '%i' does not match expected '%i'."
                |> (ConcurrencyError >> Error)

        let execAct act (state, av) =
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

        let commitEvents aid (ven : VersionedEvent<_> list) =
            (aid, ven)
            |> commit 
            |> AsyncResult.mapError (function
                | DuplicateVersionError e -> AggregateCommandFailure.ConcurrencyError e
                | PersisitenceException ex -> AggregateCommandFailure.PersistenceException ex)

        let execCmd { AggregateId = aid; AggregateVersion = av; AggregateAction = act } en =
            (av, en)
            ||> ensureVersion 
            |> Result.map (restoreState apply (zero, AggregateVersion.zero))
            |> Result.bind (execAct act)
            |> Result.bind (esureEvents aid)
            |> AsyncResult.ofResult

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
