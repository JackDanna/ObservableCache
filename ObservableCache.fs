module ObservableCache

open System
open System.Collections.Concurrent
open System.Reactive.Subjects
open FSharp.Control.Reactive
open FSharp.Control
open FsToolkit.ErrorHandling

type CacheInput<'Item, 'ItemId, 'ItemMsg> =
    | CreateItem of 'ItemId * 'Item
    | ReadItem of 'ItemId
    | UpdateItem of 'ItemId * 'ItemMsg
    | DeleteItem of 'ItemId

type Input<'Item, 'ItemId, 'ItemMsg> =
    { CorrelationId: Guid
      CacheInput: CacheInput<'Item, 'ItemId, 'ItemMsg> }

type CacheOutput<'ItemId, 'Item> =
    | CreateItemOnDB of 'ItemId * Result<'Item, string>
    | ReadItemOnDB of 'ItemId * Result<'Item, string>
    | UpdateItemOnDB of 'ItemId * Result<'Item, string>
    | DeleteItemOnDB of 'ItemId * Result<unit, string>

type Output<'ItemId, 'Item> =
    { CorrelationId: Guid
      CacheOutput: CacheOutput<'ItemId, 'Item> }

let cacheInputToItemId =
    function
    | CreateItem(itemId, _) -> itemId
    | ReadItem itemId -> itemId
    | UpdateItem(itemId, _) -> itemId
    | DeleteItem itemId -> itemId

let obsCache2
    (createOrUpdateItemOnDatabase: 'Item -> IObservable<Result<'Item, string>>)
    (readItemOnDatabase: 'ItemId -> Async<Result<'Item, string>>)
    (deleteItemOnDatabase: 'ItemId -> IObservable<Result<unit, string>>)
    (updateItem: 'ItemMsg -> 'Item -> 'Item)
    (evictionDelay: TimeSpan)
    (inputObservable: IObservable<Input<'Item, 'ItemId, 'ItemMsg>>)
    =

    let persistItemOnDatabase (itemId: 'ItemId) (result: Result<'Item, string>) =
        match result with
        | Error err -> (itemId, Error err) |> Observable.single
        | Ok item ->
            item
            |> createOrUpdateItemOnDatabase
            |> Observable.map (fun result ->
                itemId,
                result
            )

    inputObservable
    |> Observable.groupByUntil
        (_.CacheInput
         >> function
             | CreateItem(itemId, _) -> itemId
             | ReadItem itemId -> itemId
             | UpdateItem(itemId, _) -> itemId
             | DeleteItem itemId -> itemId)
        (fun group ->
            Observable.merge
                (group
                 |> Observable.filter (
                     _.CacheInput
                     >> function
                         | DeleteItem _ -> true
                         | _ -> false
                 ))
                (group |> Observable.throttle evictionDelay))
    |> Observable.bind (
        AsyncSeq.ofObservableBuffered
        >> AsyncSeq.scanAsync
            (fun (state: 'Item option, cacheOutput: Option<Guid * CacheOutput<'ItemId, 'Item>>) input ->

                match input.CacheInput with
                | CreateItem(itemId, item) ->
                    match state with
                    | None -> Some item, CreateItemOnDB(itemId, Ok item)
                    | Some _ -> Some item, CreateItemOnDB(itemId, Error "Item already exists in cache")
                    |> async.Return

                | ReadItem itemId ->
                    match state with
                    | None -> input.CacheInput |> cacheInputToItemId |> readItemOnDatabase
                    | Some item -> Ok item |> async.Return
                    |> Async.map (fun result ->
                        match result with
                        | Ok item -> Some item, ReadItemOnDB(itemId, Ok item)
                        | Error err -> None, ReadItemOnDB(itemId, Error err))

                | UpdateItem(itemId, itemMsg) ->
                    match state with
                    | None -> input.CacheInput |> cacheInputToItemId |> readItemOnDatabase
                    | Some item -> Ok item |> async.Return
                    |> Async.map (fun result ->
                        match result with
                        | Ok item ->
                            let updatedItem = updateItem itemMsg item

                            Some updatedItem, UpdateItemOnDB(itemId, Ok updatedItem)
                        | Error err -> None, UpdateItemOnDB(itemId, Error err))

                | DeleteItem itemId -> (None, DeleteItemOnDB(itemId, Ok())) |> async.Return

                |> Async.map (fun (itemOption, co) -> itemOption, Some(input.CorrelationId, co))

            )
            (None, None)
        >> AsyncSeq.toObservable
        >> Observable.choose (fun (_, processorOutputs) -> processorOutputs)
        >> fun outputObservable ->
            outputObservable
            |> Observable.takeLast 1
            |> Observable.bind (fun (correlationId, cacheOutput) ->
                match cacheOutput with
                | CreateItemOnDB(itemId, result) ->
                    persistItemOnDatabase itemId result |> Observable.map CreateItemOnDB
                | ReadItemOnDB(itemId, result) -> persistItemOnDatabase itemId result |> Observable.map ReadItemOnDB
                | UpdateItemOnDB(itemId, result) ->
                    persistItemOnDatabase itemId result |> Observable.map UpdateItemOnDB
                | DeleteItemOnDB(itemId, result) ->
                    match result with
                    | Error err -> (itemId, Error err) |> Observable.single
                    | Ok() ->
                        deleteItemOnDatabase itemId
                        |> Observable.map (fun result ->
                            itemId,
                            match result with
                            | Ok() -> Ok()
                            | Error err -> Error err)
                    |> Observable.map DeleteItemOnDB
                |> Observable.map (fun co ->
                    { CorrelationId = correlationId
                      CacheOutput = co }))
            |> Observable.subscribe (fun output -> ())
            |> ignore

            outputObservable

    )

let obsCache
    (createOrUpdateItemOnDatabase: 'Item -> IObservable<Result<'Item, string>>)
    (readItemOnDatabase: 'ItemId -> IObservable<Result<'Item, string>>)
    (deleteItemOnDatabase: 'ItemId -> IObservable<Result<unit, string>>)
    (updateItem: 'ItemMsg -> 'Item -> 'Item)
    (evictionDelay: TimeSpan)
    (inputObservable: IObservable<Input<'Item, 'ItemId, 'ItemMsg>>)
    =
    let itemCache = ConcurrentDictionary<'ItemId, 'Item>()

    let getItem itemId =
        match itemCache.TryGetValue itemId with
        | true, item -> Ok item |> Observable.single
        | false, _ ->
            readItemOnDatabase itemId
            |> Observable.map (
                Result.bind (fun item ->
                    match itemCache.TryAdd(itemId, item) with
                    | true -> Ok item
                    | false -> Error "Failed to add item to cache")
            )
        |> Observable.map (fun result -> itemId, result)

    let persistItemOnDatabase (itemId: 'ItemId) (result: Result<'Item, string>) =
        match result with
        | Error err -> (itemId, Error err) |> Observable.single
        | Ok item ->
            item
            |> createOrUpdateItemOnDatabase
            |> Observable.map (fun result ->
                itemId,
                result
                |> Result.bind (fun databaseItem ->
                    match itemCache.TryRemove itemId with
                    | true, _ -> Ok databaseItem
                    | false, _ -> Error "Failed to remove item from cache after persist"))

    inputObservable
    |> Observable.groupByUntil
        (_.CacheInput
         >> function
             | CreateItem(itemId, _) -> itemId
             | ReadItem itemId -> itemId
             | UpdateItem(itemId, _) -> itemId
             | DeleteItem itemId -> itemId)
        (fun group ->
            Observable.merge
                (group
                 |> Observable.filter (
                     _.CacheInput
                     >> function
                         | DeleteItem _ -> true
                         | _ -> false
                 ))
                (group |> Observable.throttle evictionDelay))
    |> Observable.bind (
        Observable.map (fun input ->
            match input.CacheInput with
            | CreateItem(itemId, item) ->
                (itemId,
                 match itemCache.TryAdd(itemId, item) with
                 | true -> Ok item
                 | false -> Error "Item already exists in cache")
                |> Observable.single
                |> Observable.map CreateItemOnDB
            | ReadItem itemId -> getItem itemId |> Observable.map ReadItemOnDB
            | UpdateItem(itemId, itemMsg) ->
                getItem itemId
                |> Observable.map (fun (itemId, itemResult) ->
                    itemId,
                    itemResult
                    |> Result.bind (fun item ->
                        let updatedItem = updateItem itemMsg item

                        match itemCache.TryUpdate(itemId, updatedItem, item) with
                        | true -> Ok updatedItem
                        | false -> Error "Failed to update item in cache"))
                |> Observable.map UpdateItemOnDB
            | DeleteItem itemId ->
                (itemId,
                 match itemCache.TryRemove itemId with
                 | true, _ -> Ok()
                 | false, _ -> Ok())
                |> Observable.single
                |> Observable.map DeleteItemOnDB
            |> Observable.map (fun co -> input.CorrelationId, co))
        >> Observable.concatInner
        >> Observable.publish
        >> Observable.refCount
        >> fun outputObservable ->
            outputObservable
            |> Observable.takeLast 1
            |> Observable.bind (fun (correlationId, cacheOutput) ->
                match cacheOutput with
                | CreateItemOnDB(itemId, result) ->
                    persistItemOnDatabase itemId result |> Observable.map CreateItemOnDB
                | ReadItemOnDB(itemId, result) -> persistItemOnDatabase itemId result |> Observable.map ReadItemOnDB
                | UpdateItemOnDB(itemId, result) ->
                    persistItemOnDatabase itemId result |> Observable.map UpdateItemOnDB
                | DeleteItemOnDB(itemId, result) ->
                    match result with
                    | Error err -> (itemId, Error err) |> Observable.single
                    | Ok() ->
                        deleteItemOnDatabase itemId
                        |> Observable.map (fun result ->
                            itemId,
                            match result with
                            | Ok() -> Ok()
                            | Error err -> Error err)
                    |> Observable.map DeleteItemOnDB
                |> Observable.map (fun co ->
                    { CorrelationId = correlationId
                      CacheOutput = co }))
            |> Observable.subscribe (fun output -> ())
            |> ignore

            outputObservable
    )

let createHelperFunctions
    (createOrUpdateItemOnDatabase: 'Item -> IObservable<Result<'Item, string>>)
    (readItemOnDatabase: 'ItemId -> IObservable<Result<'Item, string>>)
    (deleteItemOnDatabase: 'ItemId -> IObservable<Result<unit, string>>)
    (updateItem: 'ItemMsg -> 'Item -> 'Item)
    (evictionDelay: TimeSpan)
    =
    let rawSubject =
        new System.Reactive.Subjects.Subject<Input<'Item, 'ItemId, 'ItemMsg>>()

    let inputSubject = System.Reactive.Subjects.Subject.Synchronize rawSubject

    let outputObservable =
        obsCache
            createOrUpdateItemOnDatabase
            readItemOnDatabase
            deleteItemOnDatabase
            updateItem
            evictionDelay
            rawSubject

    let pendingItemRequests =
        ConcurrentDictionary<Guid, System.Threading.Tasks.TaskCompletionSource<Result<'Item, string>>>()

    let pendingDeleteRequests =
        ConcurrentDictionary<Guid, System.Threading.Tasks.TaskCompletionSource<Result<unit, string>>>()

    outputObservable
    |> Observable.subscribe (fun (correlationGuid, output) ->
        match output with
        | CreateItemOnDB(_, result)
        | ReadItemOnDB(_, result)
        | UpdateItemOnDB(_, result) ->
            match pendingItemRequests.TryRemove correlationGuid with
            | true, tcs -> tcs.SetResult result
            | false, _ -> ()
        | DeleteItemOnDB(_, result) ->
            match pendingDeleteRequests.TryRemove correlationGuid with
            | true, tcs -> tcs.SetResult result
            | false, _ -> ())
    |> ignore

    let dispatch msg =
        let correlationId = Guid.NewGuid()
        let tcs = System.Threading.Tasks.TaskCompletionSource<Result<'Item, string>>()
        pendingItemRequests[correlationId] <- tcs

        inputSubject.OnNext
            { CorrelationId = correlationId
              CacheInput = msg }

        tcs.Task

    let dispatchDelete msg =
        let correlationId = Guid.NewGuid()
        let tcs = System.Threading.Tasks.TaskCompletionSource<Result<unit, string>>()
        pendingDeleteRequests[correlationId] <- tcs

        inputSubject.OnNext
            { CorrelationId = correlationId
              CacheInput = msg }

        tcs.Task

    CreateItem >> dispatch, ReadItem >> dispatch, UpdateItem >> dispatch, DeleteItem >> dispatchDelete, outputObservable
