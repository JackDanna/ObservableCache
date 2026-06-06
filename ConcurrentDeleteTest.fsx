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

let dbReadLatencyMs   = 100
let dbDeleteLatencyMs = 100
let mutable dbDeleteCount = 0

let readFromDb (itemId: string) : Async<Result<string, string>> =
    async {
        do! Async.Sleep dbReadLatencyMs
        return Ok $"value-for-{itemId}"
    }

let saveToDb (item: string) : Async<Result<string, string>> =
    async.Return (Ok item)

let deleteFromDb (_itemId: string) : Async<Result<unit, string>> =
    Interlocked.Increment(&dbDeleteCount) |> ignore
    async {
        do! Async.Sleep dbDeleteLatencyMs
        return Ok ()
    }

let applyMsg (msg: string) (item: string) : string = $"{item}+{msg}"

// ---------------------------------------------------------------------------
// Build the cache
// ---------------------------------------------------------------------------

let evictionDelay = TimeSpan.FromSeconds 10.0

let create, read, update, delete, _output =
    ObservableCache.createHelperFunctions
        saveToDb
        readFromDb
        deleteFromDb
        applyMsg
        evictionDelay

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

let seed key =
    create (key, $"value-for-{key}") |> Async.AwaitTask |> Async.RunSynchronously |> ignore

// ---------------------------------------------------------------------------
// Test 1: 50 concurrent deletes on DIFFERENT keys (all should succeed)
// ---------------------------------------------------------------------------

let concurrentDeletes = 50

printfn "Starting concurrent delete test"
printfn "  Concurrent deletes : %d" concurrentDeletes
printfn "  DB delete latency  : %d ms" dbDeleteLatencyMs
printfn ""

printfn "--- Test 1: unique keys (all should succeed) ---"

for i in 0 .. concurrentDeletes - 1 do
    seed $"key-{i}"

let uniqueKeyTasks =
    Array.init concurrentDeletes (fun i ->
        task {
            let! result = delete $"key-{i}"
            return (i, result)
        })

let uniqueResults = Task.WhenAll(uniqueKeyTasks) |> Async.AwaitTask |> Async.RunSynchronously

let uniqueSuccesses = uniqueResults |> Array.filter (snd >> function Ok _ -> true  | _ -> false)
let uniqueFailures  = uniqueResults |> Array.filter (snd >> function Error _ -> true | _ -> false)

printfn "  Total    : %d" uniqueResults.Length
printfn "  Successes: %d (expected %d)" uniqueSuccesses.Length concurrentDeletes
printfn "  Failures : %d (expected 0)" uniqueFailures.Length

let test1Pass = uniqueSuccesses.Length = concurrentDeletes && uniqueFailures.Length = 0
printfn "  Result   : %s" (if test1Pass then "PASS" else "FAIL")
printfn ""

// ---------------------------------------------------------------------------
// Test 2: 50 concurrent deletes on the SAME key (all should succeed —
//         the cache treats double-delete as Ok since the end state is the same)
// ---------------------------------------------------------------------------

printfn "--- Test 2: same key (all should succeed — delete is idempotent in cache) ---"

dbDeleteCount <- 0
let sharedKey = "shared-key"
seed sharedKey

let sameKeyTasks =
    Array.init concurrentDeletes (fun i ->
        task {
            let! result = delete sharedKey
            return (i, result)
        })

let sameKeyResults = Task.WhenAll(sameKeyTasks) |> Async.AwaitTask |> Async.RunSynchronously

let sameKeySuccesses = sameKeyResults |> Array.filter (snd >> function Ok _ -> true  | _ -> false)
let sameKeyFailures  = sameKeyResults |> Array.filter (snd >> function Error _ -> true | _ -> false)

printfn "  Total    : %d" sameKeyResults.Length
printfn "  Successes: %d (expected %d)" sameKeySuccesses.Length concurrentDeletes
printfn "  Failures : %d (expected 0)" sameKeyFailures.Length
printfn "  DB deletes: %d (writes happen on eviction after group terminates)" dbDeleteCount

if sameKeyFailures.Length > 0 then
    sameKeyFailures
    |> Array.map (snd >> function Error e -> e | Ok _ -> "")
    |> Array.distinct
    |> Array.iter (printfn "  Error: %s")

// ---------------------------------------------------------------------------
// Test 3: read after delete should go back to DB (cache entry was removed)
// ---------------------------------------------------------------------------

printfn ""
printfn "--- Test 3: read after delete should fetch from DB ---"

let readAfterDeleteKey = "read-after-delete-key"
seed readAfterDeleteKey

let deleteResult =
    delete readAfterDeleteKey |> Async.AwaitTask |> Async.RunSynchronously

match deleteResult with
| Error err -> printfn "  Delete failed: %s" err
| Ok () ->
    // A short pause so the group terminates and the key is fully evicted.
    System.Threading.Thread.Sleep 500
    let mutable dbReadCount = 0
    let readResult =
        read readAfterDeleteKey |> Async.AwaitTask |> Async.RunSynchronously
    match readResult with
    | Ok v  -> printfn "  Read returned: %s" v
    | Error e -> printfn "  Read error: %s" e

let test2Pass = sameKeySuccesses.Length = concurrentDeletes
let test3Pass = match deleteResult with Ok () -> true | _ -> false

printfn ""
printfn "  Test 2 result: %s" (if test2Pass then "PASS" else "FAIL")
printfn "  Test 3 result: %s" (if test3Pass then "PASS" else "FAIL")
printfn ""
printfn "Overall: %s" (if test1Pass && test2Pass && test3Pass then "PASS" else "FAIL")
