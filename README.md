# ObservableCache

A reactive, write-through observable cache for F# backed by Rx.NET. Supports CRUD operations with correlation IDs, grouped eviction, and automatic database persistence.

## Installation

```
dotnet add package ObservableCache
```

## Overview

ObservableCache sits between your application and your database. Operations flow in as an `IObservable<Input>`, items are processed sequentially per key, and persistence to the database is handled automatically on eviction. Each operation carries a `CorrelationId` so results can be matched back to the caller.

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

let inputSubject = new Subject<Input<MyItem, Guid, MyItemMsg>>()

let outputObservable =
    obsCache
        saveToDatabase      // MyItem -> IObservable<Result<MyItem, string>>
        loadFromDatabase    // Guid   -> IObservable<Result<MyItem, string>>
        deleteFromDatabase  // Guid   -> IObservable<Result<unit, string>>
        applyMessage        // MyItemMsg -> MyItem -> MyItem
        (TimeSpan.FromSeconds 30.0)
        inputSubject
```

### High-level: `createHelperFunctions`

Returns four typed dispatch functions and the raw output observable — the recommended entry point for most use cases.

```fsharp
open ObservableCache
open System

let createItem, readItem, updateItem, deleteItem, outputObservable =
    createHelperFunctions
        saveToDatabase      // MyItem -> IObservable<Result<MyItem, string>>
        loadFromDatabase    // Guid   -> IObservable<Result<MyItem, string>>
        deleteFromDatabase  // Guid   -> IObservable<Result<unit, string>>
        applyMessage        // MyItemMsg -> MyItem -> MyItem
        (TimeSpan.FromSeconds 30.0)

// Dispatch functions return Tasks
let result: Task<Result<MyItem, string>> = createItem (id, item)
let result: Task<Result<MyItem, string>> = readItem id
let result: Task<Result<MyItem, string>> = updateItem (id, msg)
let result: Task<Result<unit,  string>> = deleteItem id

// outputObservable emits all cache operations as (CorrelationId * CacheOutput) pairs
```

## API

### Types

| Type                                   | Description                                                                       |
| -------------------------------------- | --------------------------------------------------------------------------------- |
| `CacheInput<'Item, 'ItemId, 'ItemMsg>` | Discriminated union of `CreateItem`, `ReadItem`, `UpdateItem`, `DeleteItem`       |
| `Input<'Item, 'ItemId, 'ItemMsg>`      | A `CacheInput` with a `CorrelationId: Guid`                                       |
| `CacheOutput<'ItemId, 'Item>`          | `CreateItemOnDB`, `ReadItemOnDB`, `UpdateItemOnDB`, `DeleteItemOnDB` — each carrying the `'ItemId` and a `Result` |
| `Output<'ItemId, 'Item>`               | A `CacheOutput` with a `CorrelationId: Guid`                                      |

### Functions

| Function                | Signature                                                                                                                                                                                                                                                                                           |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `obsCache`              | `('Item -> IObservable<Result<'Item, string>>) -> ('ItemId -> IObservable<Result<'Item, string>>) -> ('ItemId -> IObservable<Result<unit, string>>) -> ('ItemMsg -> 'Item -> 'Item) -> TimeSpan -> IObservable<Input<'Item, 'ItemId, 'ItemMsg>> -> IObservable<Guid * CacheOutput<'ItemId, 'Item>>` |
| `createHelperFunctions` | Same first five parameters; returns `(('ItemId * 'Item) -> Task<Result<'Item, string>>) * ('ItemId -> Task<Result<'Item, string>>) * (('ItemId * 'ItemMsg) -> Task<Result<'Item, string>>) * ('ItemId -> Task<Result<unit, string>>) * IObservable<Guid * CacheOutput<'ItemId, 'Item>>` |
