# **OneFlight** [![NuGet](https://img.shields.io/nuget/v/OneFlight.svg)](https://www.nuget.org/packages/OneFlight/)
### by [Stan Drapkin](https://github.com/sdrapkin/)

<img src="https://raw.githubusercontent.com/sdrapkin/SecurityDriven.OneFlight/refs/heads/master/assets/OneFlight.png" alt="OneFlight Logo">

## High-performance .NET single-flight/coalescing

High-performance .NET single-flight/coalescing library. Concurrent operations with the same key share one execution and one `Task`, eliminating duplicate work. Supports generic result types, `ValueTask` fast paths, state-passing overloads, custom key comparers, optional ExecutionContext suppression, and explicit `Forget` for in-flight invalidation.

**`OneFlight`** is inspired by the "single-flight" concept in Go's [`sync/singleflight`](https://pkg.go.dev/golang.org/x/sync/singleflight) package, which allows multiple concurrent requests for the same resource to be coalesced into a single request. This library brings that concept to .NET, enabling efficient handling of concurrent operations with shared keys.

`OneFlight` ensures that concurrent callers requesting the same logical operation share **one execution** and **one `Task<TResult>`**. The first caller starts the work; callers that arrive while that work is still in flight join it instead of starting duplicate work.

It is useful for suppressing request stampedes around database queries, HTTP calls, cache fills, metadata refreshes, expensive computations, and other asynchronous operations that should run only once per key at a time.

## Features

- Coalesces concurrent work by key.
- Returns the **same `Task<TResult>` instance** to the owner and all joiners.
- Lock-free dictionary read path for callers joining an existing flight.
- Avoids speculative `TaskCompletionSource` allocation during concurrent misses.
- Supports both `Task<TResult>` and `ValueTask<TResult>` loaders.
- State-passing overloads enable `static` lambdas without closure allocation.
- Supports different `TResult` types from the same `FlightGroup<TKey>`.
- Detects same-key/different-result-type collisions while a flight is active.
- Custom `IEqualityComparer<TKey>` support.
- Caller cancellation is independent from shared work.
- `Forget` can detach an in-flight operation without cancelling it.
- Optional `ExecutionContext` flow suppression.
- Completed operations are immediately removed; **`OneFlight` is not a cache**.

## Installation

```bash
dotnet add package OneFlight
```

Then:

```csharp
using SecurityDriven.OneFlight;
```

## Quick start

Create one `FlightGroup<TKey>` for the key space you want to coalesce:

```csharp
var flights = new FlightGroup<string>();
```

Use `RunAsync` around the work:

```csharp
Task<User> user = flights.RunAsync(
    $"user:{userId}",
    () => LoadUserAsync(userId));
```

If 100 callers execute that code concurrently with the same key, `LoadUserAsync(userId)` executes once. All 100 callers receive the result of that same execution.

```csharp
Task<User> a = flights.RunAsync("user:42", () => LoadUserAsync(42));
Task<User> b = flights.RunAsync("user:42", () => LoadUserAsync(42));

Debug.Assert(ReferenceEquals(a, b));
```

Once the flight completes, it is removed. A later call with the same key starts a new execution.

## How it works

For each in-flight key:

1. The first caller becomes the **owner** and supplies the loader.
2. The flight is published before the loader is invoked.
3. Concurrent callers with the same key join the existing flight.
4. The loader executes once.
5. Success, failure, or cancellation is propagated through the shared `Task<TResult>`.
6. The flight is removed when the loader completes.
7. A later caller can then start a fresh flight for that key.

Conceptually:

```text
Caller A ─┐
Caller B ─┤
Caller C ─┼── key: "user:42" ──> one loader execution ──> one shared Task<User>
Caller D ─┤
Caller E ─┘
```

`OneFlight` coalesces only **overlapping** calls. It does not memoize or cache completed results.

## First loader wins

If multiple callers race with the same key and result type, only the loader supplied by the caller that creates the flight is used:

```csharp
Task<string> first = flights.RunAsync(
    "settings",
    async () =>
    {
        await Task.Delay(100);
        return "first";
    });

Task<string> second = flights.RunAsync(
    "settings",
    () => Task.FromResult("second"));

Console.WriteLine(await first);  // first
Console.WriteLine(await second); // first
```

The second loader is never invoked.

For this reason, equal keys must represent the same logical operation.

## Caller cancellation

Caller cancellation is intentionally separate from shared-work cancellation.

A caller that no longer wants to wait should cancel **its wait**, not the shared operation:

```csharp
Task<User> shared = flights.RunAsync(
    $"user:{userId}",
    () => LoadUserAsync(userId));

User user = await shared.WaitAsync(cancellationToken);
```

If `cancellationToken` is cancelled:

- that caller stops waiting;
- the shared loader continues running;
- other callers are unaffected;
- a later caller can still join the same in-flight operation.

This avoids one impatient caller cancelling work that other callers still need.

## Loader cancellation

Cancellation produced by the loader itself is shared by all callers:

```csharp
Task<Data> task = flights.RunAsync(
    "data",
    () => LoadDataAsync(sharedCancellationToken));
```

If `LoadDataAsync` completes by throwing `OperationCanceledException`, the shared task is completed as cancelled.

Caller-specific cancellation should normally remain outside `OneFlight` via `Task.WaitAsync`.

## `Forget`

`Forget` removes the currently registered flight for a key without cancelling its work:

```csharp
Task<Value> oldTask = flights.RunAsync(
    "config",
    () => LoadOldConfigAsync());

flights.Forget("config");

Task<Value> newTask = flights.RunAsync(
    "config",
    () => LoadNewConfigAsync());
```

After `Forget`:

- existing callers still observe `oldTask`;
- the old loader continues running;
- a later caller can create a replacement flight;
- old and replacement executions may temporarily overlap;
- completion of the forgotten old flight cannot remove the replacement.

`Forget` is therefore an **in-flight invalidation/detach operation**, not cancellation.

## `ValueTask` loaders

For loaders that may complete synchronously, use `RunValueAsync`:

```csharp
Task<Config> task = flights.RunValueAsync(
    "config",
    () => TryLoadConfigAsync());
```

The public result is still a shared `Task<TResult>`:

```csharp
Task<Config> task = flights.RunValueAsync(...);
```

A synchronously completed `ValueTask<TResult>` is handled directly without using the asynchronous completion bridge.

## State-passing overloads

When a loader would otherwise need to capture state, use a state-passing overload together with a `static` lambda:

```csharp
Task<User> user = flights.RunAsync(
    $"user:{userId}",
    userId,
    static id => LoadUserAsync(id));
```

The equivalent `ValueTask` overload is:

```csharp
Task<User> user = flights.RunValueAsync(
    $"user:{userId}",
    userId,
    static id => LoadUserValueAsync(id));
```

This can avoid allocating a closure for the loader.

The public API is:

```csharp
Task<TResult> RunAsync<TResult>(
    TKey key,
    Func<Task<TResult>> loader);

Task<TResult> RunAsync<TState, TResult>(
    TKey key,
    TState state,
    Func<TState, Task<TResult>> loader);

Task<TResult> RunValueAsync<TResult>(
    TKey key,
    Func<ValueTask<TResult>> loader);

Task<TResult> RunValueAsync<TState, TResult>(
    TKey key,
    TState state,
    Func<TState, ValueTask<TResult>> loader);

void Forget(TKey key);
```

## Multiple result types

`FlightGroup` is generic only on the key:

```csharp
var flights = new FlightGroup<string>();
```

The same group can therefore serve different result types:

```csharp
User user = await flights.RunAsync(
    "user:42",
    () => LoadUserAsync(42));

Settings settings = await flights.RunAsync(
    "settings",
    () => LoadSettingsAsync());
```

However, the same key cannot simultaneously represent two different result types.

This is rejected:

```csharp
Task<string> first = flights.RunAsync(
    "same-key",
    async () =>
    {
        await Task.Delay(100);
        return "value";
    });

// Throws InvalidOperationException while the string flight is still active.
Task<int> second = flights.RunAsync(
    "same-key",
    () => Task.FromResult(42));
```

After the original flight completes, the key may be reused with another result type.

The runtime can detect a result-type collision, but it cannot detect two different logical operations that happen to use the same key and the same `TResult`. Key design remains the caller's responsibility.

## Key requirements

`TKey` is constrained as:

```csharp
where TKey : notnull
```

Keys may be reference types or value types, including composite keys:

```csharp
var flights = new FlightGroup<(int TenantId, int UserId)>();

Task<User> user = flights.RunAsync(
    (tenantId, userId),
    () => LoadUserAsync(tenantId, userId));
```

A key's effective equality and hash-code identity must remain stable while it is registered as in flight.

Avoid mutating fields that participate in `Equals` or `GetHashCode` while a key is active.

## Custom key comparers

Supply an `IEqualityComparer<TKey>` when different key-equivalence rules are needed:

```csharp
var flights = new FlightGroup<string>(
    StringComparer.OrdinalIgnoreCase);

Task<Value> a = flights.RunAsync(
    "User:42",
    () => LoadAsync());

Task<Value> b = flights.RunAsync(
    "USER:42",
    () => LoadAsync());
```

`a` and `b` join the same flight.

A custom comparer must:

- be safe for concurrent invocation;
- provide stable equality and hash-code semantics while a key is in flight;
- not throw from equality or hash-code operations.

## `ExecutionContext` behavior

By default, `FlightGroup<TKey>` suppresses `ExecutionContext` flow when starting the shared loader:

```csharp
var flights = new FlightGroup<string>(
    suppressExecutionContextFlow: true);
```

This prevents ambient state from whichever caller happens to win the race from being captured into the loader's asynchronous continuations.

For example, `AsyncLocal<T>` values from the owner are visible to the loader's synchronous prefix, but they do not flow across a subsequent asynchronous boundary when suppression is enabled.

To preserve normal .NET `ExecutionContext` flow:

```csharp
var flights = new FlightGroup<string>(
    suppressExecutionContextFlow: false);
```

Suppression affects **flow**, not the loader's synchronous execution. The loader begins inline on the owner caller's thread and its synchronous prefix sees the caller's current context.

## Loader execution

The owner invokes the loader synchronously on the calling thread.

For example:

```csharp
Task<int> task = flights.RunAsync(
    "key",
    () =>
    {
        // Runs inline before RunAsync returns.
        DoSomeSynchronousWork();

        return LoadAsync();
    });
```

Therefore, the loader's synchronous prefix should be short and non-blocking.

`OneFlight` does not wrap loaders in `Task.Run`.

## Exceptions

Exceptions from the shared loader are propagated to all callers through the shared task:

```csharp
Task<Value> a = flights.RunAsync("key", LoadAsync);
Task<Value> b = flights.RunAsync("key", LoadAsync);

try
{
    await a;
}
catch (Exception ex)
{
    // Shared loader exception.
}
```

After a faulted flight completes, the key is removed and a later call can retry.

A loader that throws synchronously is also represented as a faulted returned `Task<TResult>` rather than escaping from `RunAsync` after the flight has been established.

Argument validation errors, such as a null key or null loader, are thrown synchronously.

## Thread safety

`FlightGroup<TKey>` is designed for concurrent use.

The implementation uses a hybrid synchronization strategy:

- a `ConcurrentDictionary<TKey, ...>` provides the hot-path lookup for existing flights;
- callers joining an existing flight do not take the owner-election lock;
- a short lock is used only after a dictionary miss to elect the owner and prevent duplicate `Call`/`TaskCompletionSource` allocation;
- flight completion uses an identity-aware conditional removal so an old forgotten flight cannot remove a newer replacement.

The loader itself always executes outside the owner-election lock.

## `InFlightCount`

`FlightGroup<TKey>` exposes the number of currently registered flights:

```csharp
int count = flights.InFlightCount;
```

This is primarily useful for diagnostics and tests.

Forgotten operations that are still executing are not counted because they are no longer registered in the group.

## Example: database request coalescing

```csharp
private readonly FlightGroup<int> _users = new();

public Task<User> GetUserAsync(int userId)
{
    return _users.RunAsync(
        userId,
        static id => QueryUserAsync(id));
}

private static async Task<User> QueryUserAsync(int userId)
{
    // Potentially expensive database query.
    return await database.Users
        .SingleAsync(user => user.Id == userId);
}
```

If many requests concurrently ask for user `42`, only one `QueryUserAsync(42)` is active. Requests for user `43` use a separate flight.

## Example: external API request

```csharp
private readonly FlightGroup<string> _requests = new(
    StringComparer.Ordinal);

public Task<Product> GetProductAsync(
    string productId,
    CancellationToken cancellationToken)
{
    Task<Product> shared = _requests.RunAsync(
        $"product:{productId}",
        productId,
        static id => FetchProductAsync(id));

    return shared.WaitAsync(cancellationToken);
}
```

Each caller may independently stop waiting without cancelling the shared HTTP operation for other callers.

## Example: cache fill

`OneFlight` is especially useful alongside a cache:

```csharp
public async Task<Value> GetValueAsync(string key)
{
    if (cache.TryGetValue(key, out Value? cached))
        return cached;

    return await flights.RunAsync(
        key,
        async () =>
        {
            // Recheck after becoming the flight owner because another execution
            // may have populated the cache before this loader began.
            if (cache.TryGetValue(key, out Value? value))
                return value;

            value = await LoadValueAsync(key);
            cache[key] = value;

            return value;
        });
}
```

`OneFlight` suppresses duplicate concurrent cache fills; the cache remains responsible for retaining completed values.

## Semantics at a glance

| Situation | Behavior |
|---|---|
| First caller for a key | Creates the flight and invokes the loader |
| Concurrent caller, same key + same `TResult` | Joins the existing flight |
| Concurrent caller, same key + different `TResult` | Throws `InvalidOperationException` |
| Loader succeeds | All callers receive the result |
| Loader faults | All callers observe the shared exception |
| Loader cancels | Shared task is cancelled |
| One caller cancels `WaitAsync` | Shared work continues |
| Flight completes | Key is removed |
| Later caller after completion | Starts a new flight |
| `Forget(key)` | Detaches current flight without cancelling it |
| Old forgotten flight completes | Cannot remove a newer replacement |

## What `OneFlight` is not

`OneFlight` is deliberately small in scope.

It is **not**:
- a result cache;
- a memoization library;
- a distributed lock;
- a cross-process deduplication mechanism;
- a scheduler;
- a retry policy;
- a timeout policy;
- a shared-work cancellation coordinator.

It solves one problem: **coalescing concurrent duplicate asynchronous work within a process**.

Caching, retries, timeouts, caller cancellation, and distributed coordination can be composed around it as needed.
