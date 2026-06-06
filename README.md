# ObservableCache

A reactive, write-through observable cache for F# backed by Rx.NET. Supports CRUD operations with correlation IDs, grouped eviction, and automatic database persistence.

## Installation

```
dotnet add package ObservableCache
```

## Overview

ObservableCache sits between your application and your database. Operations flow in as an `IObservable<Input>`, items are processed concurrently across keys but sequentially per key. The persistence to the database is handled automatically on eviction. Each operation carries a `CorrelationId` so results can be matched back to the caller.

### How it works

1. **Create / Update** — items are written to the per-key cache state immediately and persisted to the database when evicted.
2. **Read** — items are served from the per-key cache state if present; otherwise fetched from the database and cached.
3. **Delete** — items are removed from the per-key cache state immediately and deleted from the database.
4. **Eviction** — items are evicted (and persisted) after a configurable idle `TimeSpan` with no activity, or immediately on delete.
5. **Concurrency** — operations for the same key are processed sequentially; operations for different keys may run concurrently.

## Usage

### Low-level: `obsCache`

Takes an `IObservable<Input>` and returns an `IObservable<Guid * CacheOutput>`.

```fsharp
open ObservableCache
open System
open System.Reactive.Subjects
open FSharp.Control.Reactive

type MyItem = { Id: Guid; Name: string }
type MyItemMsg =
    | Rename of string

let update (msg: MyItemMsg) (item: MyItem) =
    match msg with
    | Rename name -> { item with Name = name }

let inputSubject = new Subject<Input<MyItem, Guid, MyItemMsg>>()

let updateItemOnDatabase (item: MyItem) : Async<Result<MyItem, string>> =
    // Persist the item here.
    Ok item |> async.Return

let readItemOnDatabase (id: Guid) : Async<Result<MyItem, string>> =
    // Load the item here.
    Ok { Id = id; Name = "loaded" } |> async.Return

let deleteItemOnDatabase (id: Guid) : Async<Result<unit, string>> =
    // Delete the item here.
    Ok () |> async.Return

let outputObservable =
    obsCache
        updateItemOnDatabase
        readItemOnDatabase
        deleteItemOnDatabase
        update
        (TimeSpan.FromSeconds 30.0)
        inputSubject
```

### High-level: `createHelperFunctions`

Returns four typed dispatch functions and the raw output observable — the recommended entry point for most use cases.

```fsharp
open ObservableCache
open System

let id = Guid.NewGuid()
let item = { Id = id; Name = "created" }
let msg = Rename "updated"

let createItem, readItem, updateItem, deleteItem, outputObservable =
    createHelperFunctions
        updateItemOnDatabase // MyItem -> Async<Result<MyItem, string>>
        readItemOnDatabase // Guid   -> Async<Result<MyItem, string>>
        deleteItemOnDatabase  // Guid   -> Async<Result<unit, string>>
        update        // MyItemMsg -> MyItem -> MyItem
        (TimeSpan.FromSeconds 30.0)

let createResult = createItem (id, item)
let readResult = readItem id
let updateResult = updateItem (id, msg)
let deleteResult = deleteItem id

// outputObservable emits all cache operations as (CorrelationId * CacheOutput) pairs
```

## API

### Types

| Type                                     | Description                                                                                                                    |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `CacheInput<'Item, 'ItemId, 'ItemMsg>` | Discriminated union of `CreateItem`, `ReadItem`, `UpdateItem`, `DeleteItem`                                            |
| `Input<'Item, 'ItemId, 'ItemMsg>`      | A `CacheInput` with a `CorrelationId: Guid`                                                                                |
| `CacheOutput<'ItemId, 'Item>`          | `CreateItemOnDB`, `ReadItemOnDB`, `UpdateItemOnDB`, `DeleteItemOnDB` — each carrying the `'ItemId` and a `Result` |
| `Output<'ItemId, 'Item>`               | A `CacheOutput` with a `CorrelationId: Guid`                                                                               |

### Functions

| Function                  | Signature                                                                                                                                                                                                                                                                           |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `obsCache`              | `('Item -> Async<Result<'Item, string>>) -> ('ItemId -> Async<Result<'Item, string>>) -> ('ItemId -> Async<Result<unit, string>>) -> ('ItemMsg -> 'Item -> 'Item) -> TimeSpan -> IObservable<Input<'Item, 'ItemId, 'ItemMsg>> -> IObservable<Guid * CacheOutput<'ItemId, 'Item>>` |
| `createHelperFunctions` | Same first five parameters; returns typed `create`, `read`, `update`, and `delete` helper functions plus an `IObservable<Guid * CacheOutput<'ItemId, 'Item>>`                                                                                                             |

## Notes

- Database callbacks return `Async<Result<_, string>>`; ObservableCache converts them into observables internally where needed.
- Use `createHelperFunctions` when callers need typed CRUD helpers.
- Use `obsCache` directly when you already have an observable input stream and want to subscribe to all raw cache outputs.
