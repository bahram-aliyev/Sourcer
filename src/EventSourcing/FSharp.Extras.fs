namespace Sourcer.FSharp.Extras

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

    let map f (Ar : AsyncResult<_,_>) : AsyncResult<_,_> =
        Ar |> Async.map (Result.map f)

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

