#r "nuget: System.Reactive, 6.1.0"
#r "nuget: FSharp.Control.Reactive, 6.1.2"
#load "ObservableCache.fs"

open System
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Reactive

// ---------------------------------------------------------------------------
// Simulated "database" — introduces artificial latency so concurrent reads
// actually overlap and expose any race window.
// ---------------------------------------------------------------------------

let dbReadLatencyMs = 150
let mutable dbReadCount = 0

let readFromDb (itemId: string) : IObservable<Result<string, string>> =
    Interlocked.Increment(&dbReadCount) |> ignore
    Observable.ofAsync (async {
        do! Async.Sleep dbReadLatencyMs
        return Ok $"db-value-for-{itemId}"
    })

let saveToDb (item: string) : IObservable<Result<string, string>> =
    Observable.single (Ok item)

let deleteFromDb (_itemId: string) : IObservable<Result<unit, string>> =
    Observable.single (Ok ())

let applyMsg (msg: string) (item: string) : string =
    $"{item}+{msg}"

// ---------------------------------------------------------------------------
// Build the cache
// ---------------------------------------------------------------------------

let evictionDelay = TimeSpan.FromSeconds 5.0

let create, read, update, delete, _output =
    ObservableCache.createHelperFunctions
        saveToDb
        readFromDb
        deleteFromDb
        applyMsg
        evictionDelay

// ---------------------------------------------------------------------------
// Concurrent-read stress test
// ---------------------------------------------------------------------------

let concurrentReaders = 50
let testKey = "shared-key"

printfn "Starting concurrent read test"
printfn "  Key            : %s" testKey
printfn "  Concurrent reads: %d" concurrentReaders
printfn "  DB latency      : %d ms" dbReadLatencyMs
printfn ""

// Fire all reads simultaneously before any of them can complete.
let tasks =
    Array.init concurrentReaders (fun i ->
        task {
            let! result = read testKey
            return (i, result)
        })

let results = Task.WhenAll(tasks) |> Async.AwaitTask |> Async.RunSynchronously

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

let successes =
    results
    |> Array.filter (snd >> function Ok _ -> true | Error _ -> false)

let failures =
    results
    |> Array.filter (snd >> function Error _ -> true | Ok _ -> false)

printfn "Results:"
printfn "  Total    : %d" results.Length
printfn "  Successes: %d" successes.Length
printfn "  Failures : %d" failures.Length
printfn "  DB reads : %d  (ideal: 1, more = race condition)" dbReadCount
printfn ""

if failures.Length > 0 then
    printfn "FAILURES (race condition detected!):"
    for (i, result) in failures do
        match result with
        | Error err -> printfn "  Reader %d -> Error: %s" i err
        | Ok _      -> ()
    printfn ""

if dbReadCount = 1 then
    printfn "PASS: DB read exactly once — in-flight deduplication is working."
elif dbReadCount = 0 then
    printfn "UNEXPECTED: DB was never read."
else
    printfn "FAIL: DB was read %d times (expected 1) — stampede not prevented." dbReadCount

if failures.Length = 0 then
    printfn "PASS: all %d readers received a successful result." concurrentReaders
else
    printfn "FAIL: %d / %d readers received an error." failures.Length results.Length

let passed = dbReadCount = 1 && failures.Length = 0
printfn ""
printfn "Overall: %s" (if passed then "PASS" else "FAIL")
