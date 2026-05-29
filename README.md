# ObservableCache

A reactive, write-through observable cache for F# backed by Rx.NET. Supports CRUD operations with correlation IDs, grouped eviction, and automatic database persistence.

## Installation

```
dotnet add package ObservableCache
```

## Overview

ObservableCache sits between your application and your database. Operations flow in as an `IObservable<Input>`, items are cached in memory, and persistence to the database is handled automatically on eviction. Each operation carries a `CorrelationId` so results can be matched back to the caller.

### How it works

1. **Create / Update** — items are written to the in-memory cache immediately and persisted to the database when evicted.
2. **Read** — items are served from the cache if present; otherwise fetched from the database and cached.
3. **Delete** — items are removed from the cache immediately and deleted from the database.
4. **Eviction** — items are evicted (and persisted) after a configurable idle `TimeSpan` with no activity, or immediately on delete.

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

Returns four typed dispatch functions — the recommended entry point for most use cases.

```fsharp
open ObservableCache
open System

let createItem, readItem, updateItem, deleteItem =
    createHelperFunctions
        saveToDatabase      // MyItem -> IObservable<Result<MyItem, string>>
        loadFromDatabase    // Guid   -> IObservable<Result<MyItem, string>>
        deleteFromDatabase  // Guid   -> IObservable<Result<unit, string>>
        applyMessage        // MyItemMsg -> MyItem -> MyItem
        (TimeSpan.FromSeconds 30.0)

// Each returns a Task<Result<_, string>>
let result: Task<Result<MyItem, string>> = createItem (id, item)
let result: Task<Result<MyItem, string>> = readItem id
let result: Task<Result<MyItem, string>> = updateItem (id, msg)
let result: Task<Result<unit,  string>> = deleteItem id
```

## API

### Types

| Type | Description |
|------|-------------|
| `CacheInput<'Item, 'ItemId, 'ItemMsg>` | Discriminated union of `CreateItem`, `ReadItem`, `UpdateItem`, `DeleteItem` |
| `Input<'Item, 'ItemId, 'ItemMsg>` | A `CacheInput` with a `CorrelationId: Guid` |
| `CacheOutput<'ItemId, 'Item>` | Either `PersistItemOnDB` or `DeleteItemOnDB`, each carrying the id and a `Result` |
| `Output<'ItemId, 'Item>` | A `CacheOutput` with a `CorrelationId: Guid` |

### Functions

| Function | Signature |
|----------|-----------|
| `obsCache` | `('Item -> IObservable<Result<'Item, string>>) -> ('ItemId -> IObservable<Result<'Item, string>>) -> ('ItemId -> IObservable<Result<unit, string>>) -> ('ItemMsg -> 'Item -> 'Item) -> TimeSpan -> IObservable<Input<'Item, 'ItemId, 'ItemMsg>> -> IObservable<Guid * CacheOutput<'ItemId, 'Item>>` |
| `createHelperFunctions` | Same first five parameters; returns `(('ItemId * 'Item) -> Task<Result<'Item, string>>) * ('ItemId -> Task<Result<'Item, string>>) * (('ItemId * 'ItemMsg) -> Task<Result<'Item, string>>) * ('ItemId -> Task<Result<unit, string>>)` |
