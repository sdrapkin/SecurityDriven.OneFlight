using System;
using System.Threading.Tasks;

namespace SecurityDriven.OneFlight
{
	/// <summary>
	/// Coalesces concurrent executions by key. For a given in-flight key, the first caller
	/// supplies the shared loader; subsequent callers join that execution.
	/// </summary>
	/// <typeparam name="TKey">The type used to identify an in-flight operation.</typeparam>
	/// <remarks>
	/// <para>
	/// The loader is invoked synchronously on the calling thread when the call creates a new flight.
	/// Its synchronous portion should therefore be short and non-blocking.
	/// </para>
	/// <para>
	/// Caller cancellation is intentionally not part of this abstraction. The shared loader runs
	/// independently of whether individual callers still want to wait. A caller that wants to stop
	/// waiting can use <c>returnedTask.WaitAsync(cancellationToken)</c>.
	/// </para>
	/// </remarks>
	public interface IFlightGroup<TKey>
		where TKey : notnull
	{
		/// <summary>
		/// Executes <paramref name="loader"/> once for the in-flight key, or joins the
		/// existing execution for that key. The loader supplied by the first caller wins.
		/// </summary>
		Task<TResult> RunAsync<TResult>(
			TKey key,
			Func<Task<TResult>> loader);

		/// <summary>
		/// State-passing overload. Pair with a <c>static</c> lambda to avoid a closure.
		/// </summary>
		Task<TResult> RunAsync<TState, TResult>(
			TKey key,
			TState state,
			Func<TState, Task<TResult>> loader);

		/// <summary>
		/// ValueTask-based overload for loaders that may complete synchronously.
		/// The returned Task is the single shared task for the flight.
		/// </summary>
		Task<TResult> RunValueAsync<TResult>(
			TKey key,
			Func<ValueTask<TResult>> loader);

		/// <summary>
		/// State-passing ValueTask overload. Pair with a <c>static</c> lambda to avoid a closure.
		/// </summary>
		Task<TResult> RunValueAsync<TState, TResult>(
			TKey key,
			TState state,
			Func<TState, ValueTask<TResult>> loader);

		/// <summary>
		/// Removes the current in-flight execution for <paramref name="key"/>.
		/// Existing callers continue to observe the old execution. A later caller can start a new
		/// execution for the same key. This method does not cancel the old execution.
		/// </summary>
		void Forget(TKey key);
	}//IFlightGroup
}//ns
