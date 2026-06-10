#r "nuget: System.Reactive, 6.1.0"
#r "nuget: FSharp.Control.Reactive, 6.1.2"
#r "nuget: FsToolkit.ErrorHandling, 5.2.0"
#r "nuget: FSharp.Control.AsyncSeq, 3.2.1"
#load "ObservableCache.fs"

open System
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Reactive

// ---------------------------------------------------------------------------
// Simulated "database"
// ---------------------------------------------------------------------------

let dbWriteLatencyMs = 100
let mutable dbWriteCount = 0

let saveToDb (item: string) : Async<Result<string, string>> =
    Interlocked.Increment(&dbWriteCount) |> ignore

    async {
        do! Async.Sleep dbWriteLatencyMs
        return Ok item
    }

let readFromDb (_itemId: string) : Async<Result<string, string>> = async.Return(Error "item not found")

let deleteFromDb (_itemId: string) : Async<Result<unit, string>> = async.Return(Ok())

let update (msg: string) (item: string) : string = $"{item}+{msg}"

// ---------------------------------------------------------------------------
// Build the cache
// ---------------------------------------------------------------------------

let evictionDelay = TimeSpan.FromSeconds 10.0

let create, read, updateDispatch, delete, _output =
    ObservableCache.createHelperFunctions saveToDb readFromDb deleteFromDb update evictionDelay

// ---------------------------------------------------------------------------
// Test 1: 50 concurrent creates on DIFFERENT keys (all should succeed)
// ---------------------------------------------------------------------------

let concurrentCreates = 50

printfn "Starting concurrent create test"
printfn "  Concurrent creates: %d" concurrentCreates
printfn "  DB write latency  : %d ms" dbWriteLatencyMs
printfn ""

printfn "--- Test 1: unique keys (all should succeed) ---"

let uniqueKeyTasks =
    Array.init concurrentCreates (fun i ->
        task {
            let! result = create ($"key-{i}", $"value-{i}")
            return (i, result)
        })

let uniqueResults =
    Task.WhenAll(uniqueKeyTasks) |> Async.AwaitTask |> Async.RunSynchronously

let uniqueSuccesses =
    uniqueResults
    |> Array.filter (
        snd
        >> function
            | Ok _ -> true
            | _ -> false
    )

let uniqueFailures =
    uniqueResults
    |> Array.filter (
        snd
        >> function
            | Error _ -> true
            | _ -> false
    )

printfn "  Total    : %d" uniqueResults.Length
printfn "  Successes: %d (expected %d)" uniqueSuccesses.Length concurrentCreates
printfn "  Failures : %d (expected 0)" uniqueFailures.Length

let test1Pass =
    uniqueSuccesses.Length = concurrentCreates && uniqueFailures.Length = 0

printfn "  Result   : %s" (if test1Pass then "PASS" else "FAIL")
printfn ""

// ---------------------------------------------------------------------------
// Test 2: 50 concurrent creates on the SAME key (exactly 1 should succeed)
// ---------------------------------------------------------------------------

printfn "--- Test 2: same key (exactly 1 should succeed, rest should fail) ---"

dbWriteCount <- 0
let sharedKey = "shared-key"

let sameKeyTasks =
    Array.init concurrentCreates (fun i ->
        task {
            let! result = create (sharedKey, $"value-from-{i}")
            return (i, result)
        })

let sameKeyResults =
    Task.WhenAll(sameKeyTasks) |> Async.AwaitTask |> Async.RunSynchronously

let sameKeySuccesses =
    sameKeyResults
    |> Array.filter (
        snd
        >> function
            | Ok _ -> true
            | _ -> false
    )

let sameKeyFailures =
    sameKeyResults
    |> Array.filter (
        snd
        >> function
            | Error _ -> true
            | _ -> false
    )

printfn "  Total    : %d" sameKeyResults.Length
printfn "  Successes: %d (expected 1)" sameKeySuccesses.Length
printfn "  Failures : %d (expected %d)" sameKeyFailures.Length (concurrentCreates - 1)
printfn "  DB writes: %d (writes happen on eviction, not immediately)" dbWriteCount

if sameKeyFailures.Length > 0 then
    // Show unique error messages only
    sameKeyFailures
    |> Array.map (
        snd
        >> function
            | Error e -> e
            | Ok _ -> ""
    )
    |> Array.distinct
    |> Array.iter (printfn "  Error: %s")

let test2Pass = sameKeySuccesses.Length = 1
printfn "  Result   : %s" (if test2Pass then "PASS" else "FAIL")
printfn ""

// ---------------------------------------------------------------------------
// Overall
// ---------------------------------------------------------------------------

printfn "Overall: %s" (if test1Pass && test2Pass then "PASS" else "FAIL")