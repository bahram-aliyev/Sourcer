namespace Sourcer.MongoDB

open System
open System.Dynamic
open MongoDB.Driver
open Sourcer.FSharp.Extras
open Sourcer.EventSourcing
open System.Threading.Tasks
open System.Runtime.CompilerServices
open MongoDB.Bson.Serialization.Attributes


module private FSharp =

    module Extras =


        [<RequireQualifiedAccess>]
        module Result =

            let catch f =
                try
                    f () |> Ok
                with e -> Error e


        [<RequireQualifiedAccess>]
        module AsyncResult =

            let ofAsync xA : AsyncResult<_,_> = 
                xA |> Async.map Result.Ok

            let tee (f : _ -> unit) xAr = 
                AsyncResult.map f xAr |> ignore
                xAr


        [<Extension>]
        type AsyncResult() =
    
            [<Extension>]
            static member inline awaitTask (xT : Task) =
                xT |> (Async.AwaitTask >> AsyncResult.ofAsync >> AsyncResult.catch id)

            [<Extension>]
            static member inline awaitTask xT =
                xT |> (Async.AwaitTask >> AsyncResult.ofAsync >> AsyncResult.catch id)


open FSharp.Extras


type EventDocument =
    { [<BsonId>]Id : string 
      AggregateId : string 
      AggregateVersion : int 
      AggregateHash : int
      EventType : string
      Payload : ExpandoObject 
      TimestampUtc : DateTime }


type SerializeEventToDocument<'aid, 'event when 'aid : equality> = 'aid * AggregateVersion * 'event -> EventDocument


type DeserializeEventFromDocument<'event> = EventDocument -> 'event


type CollectionFactory = Type -> IMongoCollection<EventDocument>

[<RequireQualifiedAccess>]
module CollectionFactory =

    let compose (db : IMongoDatabase) : CollectionFactory =
        fun (t : Type) -> t.Name |> db.GetCollection 


type Transaction = Transaction of start : (unit -> Result<unit, exn>)
                                * commit : (unit -> AsyncResult<unit, exn>)
                                * rollback : (unit -> AsyncResult<unit, exn>)

[<RequireQualifiedAccess>]
module Transaction =


    let private composeStart (cs : IClientSessionHandle) = 
        fun () -> Result.catch (fun _ -> cs.StartTransaction())


    let private composeCommit (cs : IClientSessionHandle) =
        fun () -> 
            cs.CommitTransactionAsync() 
            |> AsyncResult.awaitTask
            |> AsyncResult.tee (fun _ -> cs.Dispose())


    let private composeRollback (cs : IClientSessionHandle) =
        fun () -> 
            cs.AbortTransactionAsync() 
            |> AsyncResult.awaitTask
            |> AsyncResult.tee (fun _ -> cs.Dispose())


    let create
        (cl : IMongoClient) 
        : AsyncResult<Transaction, exn> =

        cl.StartSessionAsync()
        |> AsyncResult.awaitTask
        |> AsyncResult.map (fun cs -> 
            Transaction(
                start = composeStart cs, 
                commit = composeCommit cs, 
                rollback = composeRollback cs))


type ClientSession = ClientSession of tr : Transaction * cf : CollectionFactory


[<RequireQualifiedAccess>]
module ClientSession =


    let create
        (db : IMongoDatabase) 
        : AsyncResult<ClientSession, exn> =

        let composeCf = CollectionFactory.compose

        db.Client
        |> Transaction.create
        |> AsyncResult.map (fun tr -> ClientSession(tr, cf = composeCf db))


[<RequireQualifiedAccess>]
module EventStore =


    let private commitTrn<'aid, 'event when 'aid : equality>
        (Transaction(start, commit, rollback)) 
        (commitEvn : ('aid * ('event * AggregateVersion) list) -> AsyncResult<unit, CommitEventsFailure>) =

        fun (aid, aven) ->
            
            let startTrn = (start >> AsyncResult.ofResult >> AsyncResult.mapError PersisitenceException)                
            
            let commitTrn = (commit >> AsyncResult.mapError PersisitenceException)
            
            let rollbackTrn cmtEr =                
                let aggregateEx (cmtFlr : CommitEventsFailure) (rlbEx : exn) =                         
                    let erMsg = sprintf "Failed to rollback failed transaction for events of type `%s` for aggregateId:%O"
                                        typeof<'event>.Name id                        
                    match cmtFlr with
                    | PersisitenceException perEx -> 
                        new AggregateException(
                                message = erMsg,
                                innerExceptions = [|perEx; rlbEx|])
                        :> Exception
                    | DuplicateVersionError msg ->
                        new AggregateException(
                                message = erMsg,
                                innerExceptions = [|(exn msg); rlbEx|])
                        :> Exception
                    |> PersisitenceException
                rollback () |> AsyncResult.mapError (aggregateEx cmtEr)

            let commitEvn () = (aid, aven) |> commitEvn
            
            let f =
                startTrn 
                >> AsyncResult.bind commitEvn
                >> AsyncResult.bind commitTrn
                >> Async.bind (function 
                    | Ok _ -> AsyncResult.retn ()
                    | Error er -> rollbackTrn er)

            f ()


    let commit<'aid, 'event when 'aid : equality>
        (cs : ClientSession)
        (ser : SerializeEventToDocument<'aid, 'event>)
        (aven : 'aid * ('event * AggregateVersion) list)
        : AsyncResult<Unit, CommitEventsFailure> = 
            
        let (ClientSession(trn, cf)) = cs

        let mapToDocuments (id, ven : ('event * AggregateVersion) list) =
            ven |> List.map (fun (e, v) -> ser (id, v, e))

        let insertDocuments (edn : EventDocument list) =
            (cf typeof<'event>).InsertManyAsync(edn, new InsertManyOptions(IsOrdered = true))
            |> (AsyncResult.awaitTask >> AsyncResult.catch id)
            |> AsyncResult.mapError (fun ex -> (edn, ex))

        let handleError =
            AsyncResult.mapError (fun (edn : EventDocument list, ex : Exception) ->
                match ex with
                | :? MongoDuplicateKeyException ->
                    sprintf "Failed to commit events [%i::%i] of `%s` for Aggregate:%s." 
                            (edn |> List.head).AggregateVersion
                            (edn |> List.last).AggregateVersion
                            typeof<'event>.Name
                            (edn |> List.head).AggregateId
                    |> DuplicateVersionError
                | ex -> PersisitenceException ex)
        
        let commit = 
            mapToDocuments 
            >> insertDocuments 
            >> handleError
        
        let f = commitTrn trn commit
        
        f aven


    let load<'aid, 'event>
        (cf : CollectionFactory)
        (des : DeserializeEventFromDocument<'event>) 
        (id : 'aid)
        : AsyncResult<'event list, LoadEventsFailure> =

        let fetchEvents id =
            let filter =
                FilterDefinitionBuilder<EventDocument>()
                    .Eq((fun x -> x.AggregateId), (id.ToString()))                        
            let findOpt = 
                let sort =
                    SortDefinitionBuilder<EventDocument>()
                        .Ascending(FieldDefinition<EventDocument>.op_Implicit("AggregateVersion"))            
                FindOptions<EventDocument>(Sort = sort)            
            let cl = cf typeof<'event>

            let readFromCursor (c : IAsyncCursor<_>) = 
                c.ToListAsync() |> AsyncResult.awaitTask
             
            cl.FindAsync(filter, findOpt)
            |> (AsyncResult.awaitTask >> AsyncResult.bind readFromCursor)

        let desEvents = Seq.map des >> List.ofSeq
        
        id
        |> fetchEvents
        |> AsyncResult.mapError LoadEventsFailure.LoadException
        |> AsyncResult.map desEvents


[<RequireQualifiedAccess>]
module EventStream =


    let compose<'aid, 'event when 'aid : equality>
        (des : DeserializeEventFromDocument<'event>)
        (cf : CollectionFactory)
        : EventStream<'aid, 'event> =

        let load = EventStore.load cf des
        EventStream load