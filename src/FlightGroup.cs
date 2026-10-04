using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

#if NET9_0_OR_GREATER
using Lock = System.Threading.Lock;
#else
using Lock = System.Object;
#endif

namespace SecurityDriven.OneFlight
{
	/// <summary>
	/// High-performance single-flight/coalescing primitive.
	/// Concurrent calls with equal keys share one execution; completed executions are not cached.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The dictionary is keyed only by <typeparamref name="TKey"/>. A single FlightGroup instance
	/// may therefore service different result types.
	/// </para>
	/// <para>
	/// If an already in-flight key is requested with a different result type, an
	/// <see cref="InvalidOperationException"/> is thrown. Exact result type identity is required.
	/// </para>
	/// <para>
	/// The caller is responsible for ensuring that equal keys identify the same logical operation.
	/// The runtime can detect a result-type collision, but it cannot detect two different loaders
	/// that intentionally or accidentally use the same key and the same result type.
	/// </para>
	/// <para>
	/// By default, ExecutionContext flow is suppressed while the shared loader is started so
	/// ambient <see cref="AsyncLocal{T}"/> state from the winning caller is not propagated into
	/// asynchronous continuations created by the shared operation. The loader's synchronous
	/// prefix still executes on the winning caller's thread and observes that caller's current context.
	/// </para>
	/// </remarks>
	public sealed class FlightGroup<TKey> : IFlightGroup<TKey>
		where TKey : notnull
	{
		// Non-generic view of a Call<TResult>, so one dictionary can hold flights for different
		// TResult types. An interface is used because Call<TResult> derives from
		// TaskCompletionSource<TResult>, which occupies the base-class slot.
		private interface ICall
		{
			Type ResultType { get; }
		}

		// Exactly one Call<TResult> exists per actual in-flight execution.
		//
		// The Call itself is the TaskCompletionSource, avoiding a separate wrapper/TCS object.
		// Its Task is returned directly to both the owner and all joiners.
		//
		// The key is retained on the Call so asynchronous completion paths do not need to carry
		// a potentially wide TKey through their state machines.
		private sealed class Call<TResult> : TaskCompletionSource<TResult>, ICall
		{
			public readonly TKey Key;

			public Call(TKey key)
				: base(TaskCreationOptions.RunContinuationsAsynchronously)
			{
				Key = key;
			}

			public Type ResultType => typeof(TResult);
		}

		// ConcurrentDictionary provides the lock-free hot read/join path.
		private readonly ConcurrentDictionary<TKey, ICall> _calls;

		// Used only after a dictionary miss. It serializes owner election so concurrent misses
		// do not speculatively allocate multiple Call<TResult>/TaskCompletionSource instances.
		private readonly Lock _ownerLock = new();

		private readonly bool _suppressExecutionContextFlow;


		/// <param name="comparer">
		/// Comparer used for key equality.
		///
		/// Because dictionary reads may execute concurrently, a custom comparer must be safe
		/// for concurrent invocation. Equality and hash-code operations must be non-throwing,
		/// and a key's effective equality/hash identity must remain stable while it is in flight.
		/// </param>
		/// <param name="suppressExecutionContextFlow">
		/// When true, suppresses ExecutionContext flow while the shared loader is started.
		/// </param>
		public FlightGroup(
			IEqualityComparer<TKey>? comparer = null,
			bool suppressExecutionContextFlow = true)
		{
			_calls = new ConcurrentDictionary<TKey, ICall>(
				comparer ?? EqualityComparer<TKey>.Default);

			_suppressExecutionContextFlow = suppressExecutionContextFlow;
		}

		/// <summary>
		/// Number of keys currently registered as in flight.
		/// Forgotten executions that continue running are not included.
		/// </summary>
		public int InFlightCount => _calls.Count;

		// -----------------------------------------------------------------------------------------
		// Task loaders
		// -----------------------------------------------------------------------------------------

		public Task<TResult> RunAsync<TResult>(
			TKey key,
			Func<Task<TResult>> loader)
		{
			ArgumentNullException.ThrowIfNull(loader);

			return RunCore<Func<Task<TResult>>, TResult>(
				key,
				loader,
				static f => new ValueTask<TResult>(f()));
		}

		public Task<TResult> RunAsync<TState, TResult>(
			TKey key,
			TState state,
			Func<TState, Task<TResult>> loader)
		{
			ArgumentNullException.ThrowIfNull(loader);

			return RunCore<(Func<TState, Task<TResult>> Loader, TState State), TResult>(
				key,
				(loader, state),
				static s => new ValueTask<TResult>(s.Loader(s.State)));
		}

		// -----------------------------------------------------------------------------------------
		// ValueTask loaders
		// -----------------------------------------------------------------------------------------

		public Task<TResult> RunValueAsync<TResult>(
			TKey key,
			Func<ValueTask<TResult>> loader)
		{
			ArgumentNullException.ThrowIfNull(loader);

			return RunCore<Func<ValueTask<TResult>>, TResult>(
				key,
				loader,
				static f => f());
		}

		public Task<TResult> RunValueAsync<TState, TResult>(
			TKey key,
			TState state,
			Func<TState, ValueTask<TResult>> loader)
		{
			ArgumentNullException.ThrowIfNull(loader);

			return RunCore(
				key,
				state,
				loader);
		}

		// -----------------------------------------------------------------------------------------
		// Forget
		// -----------------------------------------------------------------------------------------

		public void Forget(TKey key)
		{
			ValidateKey(key);

			// Deliberately unconditional: detach whichever flight is currently registered for this key.
			_calls.TryRemove(key, out _);
		}

		// -----------------------------------------------------------------------------------------
		// Core
		// -----------------------------------------------------------------------------------------

		private Task<TResult> RunCore<TState, TResult>(
			TKey key,
			TState state,
			Func<TState, ValueTask<TResult>> loader)
		{
			ValidateKey(key);

			// -------------------------------------------------------------------------------------
			// HOT PATH: Most calls to a heavily contended flight should land here.
			// ConcurrentDictionary reads do not require the owner-election lock.
			// -------------------------------------------------------------------------------------
			if (_calls.TryGetValue(key, out ICall? existing))
			{
				return Join<TResult>(existing).Task;
			}

			// -------------------------------------------------------------------------------------
			// MISS PATH:  Serialize only creation/owner election.
			// The second lookup is essential: another caller may have created the flight between
			// our lock-free TryGetValue miss and acquisition of _ownerLock.
			//
			// Importantly, Call<TResult> is not allocated until after this second lookup. Therefore
			// a burst of callers racing on an absent key still creates exactly one Call/TCS/Task.
			// -------------------------------------------------------------------------------------
			Call<TResult> call;
			lock (_ownerLock)
			{
				if (_calls.TryGetValue(key, out existing))
				{
					return Join<TResult>(existing).Task;
				}

				call = new Call<TResult>(key);


				// All additions performed by FlightGroup pass through _ownerLock, so once the
				// second lookup above reports the key absent, no other FlightGroup caller can
				// concurrently insert the same key.
				//
				// TryAdd is still preferable to an indexer assignment because it cannot silently
				// overwrite an existing flight should this invariant ever be violated.
				if (!_calls.TryAdd(key, call))
				{
					// This path should not be reachable under the class's insertion discipline.
					// Recover if possible rather than returning an unregistered Call.
					if (_calls.TryGetValue(key, out existing))
					{
						return Join<TResult>(existing).Task;
					}
					throw new InvalidOperationException("Failed to register a new in-flight operation for the key.");
				}
			}//lock

			// We do not invoke arbitrary user code while holding _ownerLock.
			//
			// After the Call is published, joiners may immediately obtain its Task even before
			// StartOwner executes. That is intentional: they simply wait on the same shared Task.
			StartOwner(call, state, loader);

			// The exact same Task<TResult> instance is returned to the owner and every joiner.
			return call.Task;
		}

		private static Call<TResult> Join<TResult>(ICall existingCall)
		{
			if (existingCall is Call<TResult> typed)
			{
				return typed;
			}

			throw new InvalidOperationException(
				"An in-flight operation for this key already uses result type " +
				$"'{existingCall.ResultType.FullName}', but this call requested " +
				$"'{typeof(TResult).FullName}'. Ensure equal keys identify the same " +
				"logical operation and result type.");
		}

		private static void ValidateKey(TKey key)
		{
			// Avoid boxing value-type keys.
			if (default(TKey) is null)
			{
				ArgumentNullException.ThrowIfNull(key);
			}
		}

		// -----------------------------------------------------------------------------------------
		// Shared work
		// -----------------------------------------------------------------------------------------

		private void StartOwner<TState, TResult>(
			Call<TResult> call,
			TState state,
			Func<TState, ValueTask<TResult>> loader)
		{
			try
			{
				if (_suppressExecutionContextFlow &&
					!ExecutionContext.IsFlowSuppressed())
				{
					using (ExecutionContext.SuppressFlow())
					{
						StartOwnerCore(call, state, loader);
					}
				}
				else
				{
					StartOwnerCore(call, state, loader);
				}
			}
			catch (OperationCanceledException ex)
			{
				Finish(call);
				SetCanceled(call, ex);
			}
			catch (Exception ex)
			{
				Finish(call);
				call.TrySetException(ex);

				// Observe the exception in case nobody ever awaits the shared Task.
				_ = call.Task.Exception;
			}
		}

		private void StartOwnerCore<TState, TResult>(
			Call<TResult> call,
			TState state,
			Func<TState, ValueTask<TResult>> loader)
		{
			ValueTask<TResult> valueTask = loader(state);

			// IsCompleted intentionally handles all synchronous terminal states:
			// success, fault, and cancellation. GetResult propagates fault/cancellation
			// into StartOwner's surrounding exception handling.
			if (valueTask.IsCompleted)
			{
				TResult result = valueTask.GetAwaiter().GetResult();

				Finish(call);
				call.TrySetResult(result);
				return;
			}

			// Internal bridge from the loader's ValueTask to the single shared Task.
			_ = CompleteAsync(call, valueTask);
		}

		private static void SetCanceled<TResult>(
			TaskCompletionSource<TResult> tcs,
			OperationCanceledException exception)
		{
			if (exception.CancellationToken.CanBeCanceled)
			{
				tcs.TrySetCanceled(exception.CancellationToken);
			}
			else
			{
				tcs.TrySetCanceled();
			}
		}

		private async Task CompleteAsync<TResult>(
			Call<TResult> call,
			ValueTask<TResult> valueTask)
		{
			try
			{
				TResult result = await valueTask.ConfigureAwait(false);

				// Remove the key before completing the shared Task so a caller arriving after the
				// loader has completed starts a fresh flight rather than observing a cached result.
				Finish(call);
				call.TrySetResult(result);
			}
			catch (OperationCanceledException ex)
			{
				Finish(call);
				SetCanceled(call, ex);
			}
			catch (Exception ex)
			{
				Finish(call);
				call.TrySetException(ex);

				// Observe the exception in case nobody ever awaits the shared Task.
				_ = call.Task.Exception;
			}
		}

		// -----------------------------------------------------------------------------------------
		// Completion
		// -----------------------------------------------------------------------------------------

		private void Finish<TResult>(Call<TResult> call)
		{
			// Atomic conditional removal.
			//
			// Forget() may already have detached this Call and a later caller may have installed
			// a replacement for the same key. We must therefore remove the dictionary entry only
			// if BOTH:
			//
			//   1. the key matches; and
			//   2. the currently registered value is this Call.
			//
			// ConcurrentDictionary.TryRemove(KeyValuePair<k,v>) supplies exactly those semantics.
			//
			// ICall uses normal reference equality here because the only implementation is the
			// sealed Call<TResult>, which does not override equality.
			_calls.TryRemove(new KeyValuePair<TKey, ICall>(call.Key, call));
		}
	}
}//ns
