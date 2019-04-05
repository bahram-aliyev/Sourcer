namespace Sourcer.EventSourcing


[<AutoOpen>]
module Types =

    open System
    open Sourcer.FSharp.Extras


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
        | PersistenceException of Exception


    type LoadStateFailure =
        | LoadException of Exception
        | StateNotExist of aggregateId : string * version : int * eventType : Type
        | NotFound of aggregateId : string * eventType : Type


    type CommitEventsFailure =
        | DuplicateVersionError of ErrorMessage
        | PersisitenceException of Exception


    type LoadEventsFailure = 
        | LoadException of Exception


    type EventStore<'aid, 'event> = 
            EventStore of commit : (('aid * ('event * AggregateVersion) list) -> AsyncResult<unit, CommitEventsFailure>)
                        * load : ('aid -> AsyncResult<'event list, LoadEventsFailure>)


    type EventStream<'AggregateId, 'Event> =
            EventStream of load : ('AggregateId * AggregateVersion option -> AsyncResult<'Event list, LoadEventsFailure>)
