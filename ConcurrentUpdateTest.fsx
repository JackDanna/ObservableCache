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
// Simulated "database" — latency ensures concurrent updates truly overlap.
// ---------------------------------------------------------------------------

let dbReadLatencyMs  = 100
let dbWriteLatencyMs = 100
let mutable dbReadCount  = 0
let mutable dbWriteCount = 0

let readFromDb (itemId: string) : Async<Result<string, string>> =
    Interlocked.Increment(&dbReadCount) |> ignore
    async {
        do! Async.Sleep dbReadLatencyMs
        return Ok $"base"
    }

let saveToDb (item: string) : Async<Result<string, string>> =
    Interlocked.Increment(&dbWriteCount) |> ignore
    async {
        do! Async.Sleep dbWriteLatencyMs
        return Ok item
    }

let deleteFromDb (_itemId: string) : Async<Result<unit, string>> =
    async.Return (Ok ())

// Each update message appends "+N" so we can reconstruct the expected final value.
let update (msg: string) (item: string) : string =
    $"{item}+{msg}"

// ---------------------------------------------------------------------------
// Build the cache
// ---------------------------------------------------------------------------

let evictionDelay = TimeSpan.FromSeconds 30.0

let create, read, updateDispatch, delete, _output =
    ObservableCache.createHelperFunctions
        saveToDb
        readFromDb
        deleteFromDb
        update
        evictionDelay

// ---------------------------------------------------------------------------
// Concurrent-update stress test
// ---------------------------------------------------------------------------

let concurrentUpdaters = 50
let testKey = "shared-key"

printfn "Starting concurrent update test"
printfn "  Key               : %s" testKey
printfn "  Concurrent updates: %d" concurrentUpdaters
printfn "  DB read latency   : %d ms" dbReadLatencyMs
printfn "  DB write latency  : %d ms" dbWriteLatencyMs
printfn ""

// Seed the cache with the item first so updates don't also race on a cold read.
let seedResult =
    create (testKey, "base") |> Async.AwaitTask |> Async.RunSynchronously

match seedResult with
| Error err -> failwithf "Failed to seed item: %s" err
| Ok _ -> printfn "Seeded item with value 'base'"

// Fire all updates simultaneously.
let tasks =
    Array.init concurrentUpdaters (fun i ->
        task {
            let! result = updateDispatch (testKey, string i)
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

// Verify the final cached value contains all applied messages.
// Each successful update should have appended "+N" exactly once.
let finalValues =
    successes |> Array.map (snd >> function Ok v -> v | Error _ -> "")

let lastValue =
    finalValues |> Array.tryLast |> Option.defaultValue "(none)"

// Count how many distinct "+N" tokens appear in the last reported value.
let appliedUpdates =
    Array.init concurrentUpdaters (fun i -> $"+{i}")
    |> Array.filter (fun token -> lastValue.Contains token)

printfn ""
printfn "Results:"
printfn "  Total    : %d" results.Length
printfn "  Successes: %d" successes.Length
printfn "  Failures : %d" failures.Length
printfn "  DB reads : %d  (ideal: 0, cache was pre-seeded)" dbReadCount
printfn "  DB writes: %d  (one eviction write per update group is normal)" dbWriteCount
printfn "  Final value (last success): %s" lastValue
printfn "  Applied updates found in final value: %d / %d" appliedUpdates.Length concurrentUpdaters
printfn ""

if failures.Length > 0 then
    printfn "FAILURES:"
    for (i, result) in failures do
        match result with
        | Error err -> printfn "  Updater %d -> Error: %s" i err
        | Ok _ -> ()
    printfn ""

let allSucceeded = failures.Length = 0
let allUpdatesApplied = appliedUpdates.Length = concurrentUpdaters

if allSucceeded then
    printfn "PASS: all %d updaters received a successful result." concurrentUpdaters
else
    printfn "FAIL: %d / %d updaters received an error." failures.Length results.Length

if allUpdatesApplied then
    printfn "PASS: all %d updates are reflected in the final value." concurrentUpdaters
else
    printfn "WARN: only %d / %d updates found in the final value — some may have been lost." appliedUpdates.Length concurrentUpdaters
    printfn "      (This can happen if TryUpdate lost a race — lost-update anomaly.)"

printfn ""
printfn "Overall: %s" (if allSucceeded && allUpdatesApplied then "PASS" else "FAIL")
