using System.Diagnostics;
#nullable enable

namespace SecurityDriven.OneFlight.Tests
{
	[TestClass]
	public sealed class FlightGroupTests
	{
		// -----------------------------------------------------------------------------------------
		// Basic coalescing
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task ConcurrentSameKey_ExecutesLoaderOnce_AndSharesSameTask()
		{
			var flights = new FlightGroup<string>();

			int executions = 0;

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<int> Load()
			{
				Interlocked.Increment(ref executions);
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return 42;
			}

			var calls = new Task<int>[32];

			calls[0] = flights.RunAsync("user:42", Load);

			await started.Task.ConfigureAwait(false);

			for (int i = 1; i < calls.Length; i++)
			{
				calls[i] = flights.RunAsync(
					"user:42",
					static () => Task.FromResult(999));
			}

			Assert.AreEqual(
				1,
				executions,
				"Exactly one loader should execute.");

			for (int i = 1; i < calls.Length; i++)
			{
				Assert.AreSame(
					calls[0],
					calls[i],
					$"Caller {i} should receive the exact same Task instance.");
			}

			Assert.AreEqual(
				1,
				flights.InFlightCount,
				"The flight should remain registered while the loader is running.");

			release.SetResult(true);

			int[] results = await Task.WhenAll(calls).ConfigureAwait(false);

			Assert.IsTrue(
				results.All(static x => x == 42),
				"All callers should receive the owner's result.");

			Assert.AreEqual(
				0,
				flights.InFlightCount,
				"The flight should be removed after completion.");
		}


		[TestMethod]
		public async Task CompletedFlight_IsNotCached()
		{
			var flights = new FlightGroup<string>();

			int executions = 0;

			Task<string> First()
			{
				Interlocked.Increment(ref executions);
				return Task.FromResult("first");
			}

			Task<string> firstTask = flights.RunAsync(
				"late",
				First);

			Assert.AreEqual(
				"first",
				await firstTask.ConfigureAwait(false));

			Assert.AreEqual(
				0,
				flights.InFlightCount);

			Task<string> secondTask = flights.RunAsync(
				"late",
				() =>
				{
					Interlocked.Increment(ref executions);
					return Task.FromResult("second");
				});

			Assert.AreEqual(
				"second",
				await secondTask.ConfigureAwait(false));

			Assert.AreEqual(
				2,
				executions,
				"A completed flight must not be cached.");

			Assert.AreNotSame(
				firstTask,
				secondTask);
		}


		[TestMethod]
		public async Task DifferentKeys_ExecuteIndependentlyAndConcurrently()
		{
			var flights = new FlightGroup<string>();

			int active = 0;
			int maxActive = 0;

			var startedA = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var startedB = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> LoadA()
			{
				int now = Interlocked.Increment(ref active);
				UpdateMax(ref maxActive, now);

				startedA.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				Interlocked.Decrement(ref active);

				return "A";
			}

			async Task<string> LoadB()
			{
				int now = Interlocked.Increment(ref active);
				UpdateMax(ref maxActive, now);

				startedB.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				Interlocked.Decrement(ref active);

				return "B";
			}

			Task<string> a = flights.RunAsync("key:A", LoadA);
			Task<string> b = flights.RunAsync("key:B", LoadB);

			await Task.WhenAll(
				startedA.Task,
				startedB.Task).ConfigureAwait(false);

			Assert.AreEqual(
				2,
				maxActive,
				"Different keys should be able to execute concurrently.");

			Assert.AreNotSame(a, b);

			Assert.AreEqual(
				2,
				flights.InFlightCount);

			release.SetResult(true);

			Assert.AreEqual(
				"A",
				await a.ConfigureAwait(false));

			Assert.AreEqual(
				"B",
				await b.ConfigureAwait(false));

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		[TestMethod]
		public async Task SameKey_FirstLoaderWins_JoinerLoaderIsNotInvoked()
		{
			var flights = new FlightGroup<string>();

			int firstExecutions = 0;
			int secondExecutions = 0;

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> First()
			{
				Interlocked.Increment(ref firstExecutions);
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return "first-loader";
			}

			Task<string> first = flights.RunAsync(
				"winner",
				First);

			await started.Task.ConfigureAwait(false);

			Task<string> second = flights.RunAsync(
				"winner",
				() =>
				{
					Interlocked.Increment(ref secondExecutions);
					return Task.FromResult("second-loader");
				});

			Assert.AreSame(
				first,
				second,
				"Same-key callers should share the exact same Task.");

			Assert.AreEqual(
				0,
				secondExecutions,
				"The joiner's loader must never execute.");

			release.SetResult(true);

			Assert.AreEqual(
				"first-loader",
				await first.ConfigureAwait(false));

			Assert.AreEqual(
				1,
				firstExecutions);

			Assert.AreEqual(
				0,
				secondExecutions);
		}


		// -----------------------------------------------------------------------------------------
		// TResult handling
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task SameKey_DifferentResultType_ThrowsWhileFlightIsActive()
		{
			var flights = new FlightGroup<string>();

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> LoadString()
			{
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return "text";
			}

			Task<string> textTask = flights.RunAsync(
				"type-collision",
				LoadString);

			await started.Task.ConfigureAwait(false);

			AssertThrowsSynchronously<InvalidOperationException>(
				() =>
				{
					_ = flights.RunAsync(
						"type-collision",
						static () => Task.FromResult(123));
				});

			Assert.AreEqual(
				1,
				flights.InFlightCount);

			release.SetResult(true);

			Assert.AreEqual(
				"text",
				await textTask.ConfigureAwait(false));

			// Reusing the same key with another TResult is valid after completion.
			Task<int> afterCompletion = flights.RunAsync(
				"type-collision",
				static () => Task.FromResult(123));

			Assert.AreEqual(
				123,
				await afterCompletion.ConfigureAwait(false));
		}


		[TestMethod]
		public async Task Forget_AllowsReplacementWithDifferentResultType()
		{
			var flights = new FlightGroup<string>();

			var oldStarted = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var oldRelease = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var replacementStarted = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var replacementRelease = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> Old()
			{
				oldStarted.TrySetResult(true);

				await oldRelease.Task.ConfigureAwait(false);

				return "old";
			}

			async Task<int> Replacement()
			{
				replacementStarted.TrySetResult(true);

				await replacementRelease.Task.ConfigureAwait(false);

				return 123;
			}

			Task<string> oldTask = flights.RunAsync(
				"mixed-type-forget",
				Old);

			await oldStarted.Task.ConfigureAwait(false);

			flights.Forget("mixed-type-forget");

			Task<int> replacementTask = flights.RunAsync(
				"mixed-type-forget",
				Replacement);

			await replacementStarted.Task.ConfigureAwait(false);

			Assert.AreEqual(
				1,
				flights.InFlightCount);

			// Complete the forgotten string flight while the int flight remains registered.
			oldRelease.SetResult(true);

			Assert.AreEqual(
				"old",
				await oldTask.ConfigureAwait(false));

			Assert.AreEqual(
				1,
				flights.InFlightCount,
				"Completion of the old TResult flight must not remove the replacement.");

			Task<int> joiner = flights.RunAsync(
				"mixed-type-forget",
				static () => Task.FromResult(999));

			Assert.AreSame(
				replacementTask,
				joiner);

			replacementRelease.SetResult(true);

			Assert.AreEqual(
				123,
				await replacementTask.ConfigureAwait(false));

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		// -----------------------------------------------------------------------------------------
		// Failure and cancellation
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task Failure_IsShared_AndRetrySucceeds()
		{
			var flights = new FlightGroup<string>();

			int executions = 0;

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> Fail()
			{
				Interlocked.Increment(ref executions);
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				throw new InvalidOperationException("boom");
			}

			Task<string> first = flights.RunAsync(
				"failure",
				Fail);

			await started.Task.ConfigureAwait(false);

			Task<string> second = flights.RunAsync(
				"failure",
				static () => Task.FromResult("ignored"));

			Assert.AreSame(first, second);

			release.SetResult(true);

			InvalidOperationException ex1 =
				await CaptureExceptionAsync<InvalidOperationException>(first)
					.ConfigureAwait(false);

			InvalidOperationException ex2 =
				await CaptureExceptionAsync<InvalidOperationException>(second)
					.ConfigureAwait(false);

			Assert.AreEqual("boom", ex1.Message);
			Assert.AreEqual("boom", ex2.Message);

			Assert.AreEqual(
				1,
				executions);

			Assert.AreEqual(
				0,
				flights.InFlightCount);

			Task<string> retry = flights.RunAsync(
				"failure",
				static () => Task.FromResult("recovered"));

			Assert.AreEqual(
				"recovered",
				await retry.ConfigureAwait(false));
		}


		[TestMethod]
		public async Task LoaderGeneratedCancellation_ProducesCanceledSharedTask()
		{
			var flights = new FlightGroup<string>();

			using var cancellation = new CancellationTokenSource();

			cancellation.Cancel();

			Task<int> canceled = flights.RunAsync(
				"loader-cancel",
				() => Task.FromCanceled<int>(cancellation.Token));

			Assert.IsTrue(
				canceled.IsCanceled);

			OperationCanceledException exception =
				await CaptureExceptionAsync<OperationCanceledException>(canceled)
					.ConfigureAwait(false);

			Assert.AreEqual(
				cancellation.Token,
				exception.CancellationToken,
				"The loader cancellation token should be preserved.");

			Assert.AreEqual(
				0,
				flights.InFlightCount);

			// Regression: parameterless OCE has CancellationToken.None.
			Task<int> canceledDefaultToken = flights.RunAsync<int>(
				"loader-cancel-default-token",
				static () => throw new OperationCanceledException());

			Assert.IsTrue(
				canceledDefaultToken.IsCanceled);

			await CaptureExceptionAsync<OperationCanceledException>(
				canceledDefaultToken).ConfigureAwait(false);

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		[TestMethod]
		public async Task FaultedTaskContainingOperationCanceledException_IsNormalizedToCancellation()
		{
			var flights = new FlightGroup<string>();

			Task<int> task = flights.RunAsync(
				"faulted-oce",
				static () => Task.FromException<int>(
					new OperationCanceledException()));

			await CaptureExceptionAsync<OperationCanceledException>(task)
				.ConfigureAwait(false);

			Assert.IsTrue(
				task.IsCanceled,
				"OperationCanceledException should be represented as shared cancellation.");

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		[TestMethod]
		public async Task CallerWaitCancellation_DoesNotCancelSharedFlight()
		{
			var flights = new FlightGroup<string>();

			int executions = 0;

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> Load()
			{
				Interlocked.Increment(ref executions);
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return "shared-result";
			}

			Task<string> shared = flights.RunAsync(
				"caller-cancel",
				Load);

			await started.Task.ConfigureAwait(false);

			using var callerCancellation = new CancellationTokenSource();

			Task<string> canceledWait =
				shared.WaitAsync(callerCancellation.Token);

			callerCancellation.Cancel();

			await CaptureExceptionAsync<OperationCanceledException>(
				canceledWait).ConfigureAwait(false);

			Assert.AreEqual(
				1,
				executions);

			Assert.AreEqual(
				1,
				flights.InFlightCount,
				"Caller cancellation must not remove the shared flight.");

			Task<string> joiner = flights.RunAsync(
				"caller-cancel",
				static () => Task.FromResult("ignored"));

			Assert.AreSame(
				shared,
				joiner);

			release.SetResult(true);

			Assert.AreEqual(
				"shared-result",
				await shared.ConfigureAwait(false));

			Assert.AreEqual(
				"shared-result",
				await joiner.ConfigureAwait(false));

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		// -----------------------------------------------------------------------------------------
		// Forget
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task Forget_OldCompletionCannotRemoveInFlightReplacement()
		{
			var flights = new FlightGroup<string>();

			var oldStarted = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var oldRelease = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var replacementStarted = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var replacementRelease = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> Old()
			{
				oldStarted.TrySetResult(true);

				await oldRelease.Task.ConfigureAwait(false);

				return "old";
			}

			async Task<string> Replacement()
			{
				replacementStarted.TrySetResult(true);

				await replacementRelease.Task.ConfigureAwait(false);

				return "new";
			}

			Task<string> oldTask = flights.RunAsync(
				"forget",
				Old);

			await oldStarted.Task.ConfigureAwait(false);

			Assert.AreEqual(
				1,
				flights.InFlightCount);

			flights.Forget("forget");

			Assert.AreEqual(
				0,
				flights.InFlightCount,
				"Forget should immediately detach the old flight.");

			Task<string> replacementTask = flights.RunAsync(
				"forget",
				Replacement);

			await replacementStarted.Task.ConfigureAwait(false);

			Assert.AreEqual(
				1,
				flights.InFlightCount,
				"The replacement should be registered.");

			// Complete OLD while REPLACEMENT is still registered.
			oldRelease.SetResult(true);

			Assert.AreEqual(
				"old",
				await oldTask.ConfigureAwait(false));

			Assert.AreEqual(
				1,
				flights.InFlightCount,
				"The forgotten old flight must not remove the replacement.");

			int unexpectedLoaderExecutions = 0;

			Task<string> joiner = flights.RunAsync(
				"forget",
				() =>
				{
					Interlocked.Increment(ref unexpectedLoaderExecutions);
					return Task.FromResult("should-not-run");
				});

			Assert.AreSame(
				replacementTask,
				joiner,
				"A caller after old completion should still join the replacement.");

			Assert.AreEqual(
				0,
				unexpectedLoaderExecutions,
				"The joiner's loader must not execute.");

			replacementRelease.SetResult(true);

			Assert.AreEqual(
				"new",
				await replacementTask.ConfigureAwait(false));

			Assert.AreEqual(
				"new",
				await joiner.ConfigureAwait(false));

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		[TestMethod]
		public void Forget_MissingKey_IsHarmless()
		{
			var flights = new FlightGroup<string>();

			flights.Forget("does-not-exist");

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		// -----------------------------------------------------------------------------------------
		// Argument and loader behavior
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task ArgumentValidation_IsSynchronous_AndSynchronousLoaderFailureIsReturnedAsTask()
		{
			var flights = new FlightGroup<string>();

			AssertThrowsSynchronously<ArgumentNullException>(
				() =>
				{
					_ = flights.RunAsync<string>(
						null!,
						static () => Task.FromResult("x"));
				});

			AssertThrowsSynchronously<ArgumentNullException>(
				() =>
				{
					_ = flights.RunAsync<string>(
						"null-loader",
						null!);
				});

			AssertThrowsSynchronously<ArgumentNullException>(
				() =>
				{
					_ = flights.RunValueAsync<string>(
						"null-value-loader",
						null!);
				});

			AssertThrowsSynchronously<ArgumentNullException>(
				() =>
				{
					_ = flights.RunAsync<int, string>(
						"null-state-loader",
						1,
						null!);
				});

			AssertThrowsSynchronously<ArgumentNullException>(
				() =>
				{
					_ = flights.RunValueAsync<int, string>(
						"null-value-state-loader",
						1,
						null!);
				});

			AssertThrowsSynchronously<ArgumentNullException>(
				() =>
				{
					flights.Forget(null!);
				});

			Task<int> synchronousThrow = flights.RunAsync<int>(
				"sync-throw",
				static () => throw new ApplicationException("sync boom"));

			Assert.IsTrue(
				synchronousThrow.IsFaulted,
				"A synchronous loader exception should fault the returned Task.");

			ApplicationException exception =
				await CaptureExceptionAsync<ApplicationException>(
					synchronousThrow).ConfigureAwait(false);

			Assert.AreEqual(
				"sync boom",
				exception.Message);

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		[TestMethod]
		public async Task TaskLoaderReturningNull_FaultsFlight_AndReleasesKey()
		{
			var flights = new FlightGroup<string>();

			Task<int> task = flights.RunAsync<int>(
				"null-task",
				static () => null!);

			Assert.IsTrue(
				task.IsFaulted);

			await CaptureExceptionAsync<ArgumentNullException>(task)
				.ConfigureAwait(false);

			Assert.AreEqual(
				0,
				flights.InFlightCount);

			Task<int> retry = flights.RunAsync(
				"null-task",
				static () => Task.FromResult(42));

			Assert.AreEqual(
				42,
				await retry.ConfigureAwait(false));
		}


		[TestMethod]
		public async Task SynchronousLoaderPrefix_ExecutesInlineOnCallingThread()
		{
			var flights = new FlightGroup<string>();

			int callerThread = Environment.CurrentManagedThreadId;
			int loaderThread = 0;

			Stopwatch stopwatch = Stopwatch.StartNew();

			Task<int> task = flights.RunAsync(
				"inline",
				() =>
				{
					loaderThread = Environment.CurrentManagedThreadId;

					Thread.Sleep(40);

					return Task.FromResult(7);
				});

			stopwatch.Stop();

			Assert.AreEqual(
				callerThread,
				loaderThread,
				"The loader's synchronous prefix should run on the calling thread.");

			Assert.IsTrue(
				stopwatch.ElapsedMilliseconds >= 30,
				"RunAsync should not offload the loader's synchronous prefix.");

			Assert.AreEqual(
				7,
				await task.ConfigureAwait(false));
		}


		// -----------------------------------------------------------------------------------------
		// ValueTask
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task ValueTaskLoaders_SupportSynchronousAndAsynchronousCompletion()
		{
			var flights = new FlightGroup<string>();

			Task<int> synchronous = flights.RunValueAsync(
				"vt:sync",
				static () => new ValueTask<int>(123));

			Assert.IsTrue(
				synchronous.IsCompletedSuccessfully);

			Assert.AreEqual(
				123,
				await synchronous.ConfigureAwait(false));

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async ValueTask<int> AsyncValue()
			{
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return 456;
			}

			Task<int> asyncTask = flights.RunValueAsync(
				"vt:async",
				AsyncValue);

			await started.Task.ConfigureAwait(false);

			Task<int> joiner = flights.RunValueAsync(
				"vt:async",
				static () => new ValueTask<int>(999));

			Assert.AreSame(
				asyncTask,
				joiner);

			release.SetResult(true);

			Assert.AreEqual(
				456,
				await joiner.ConfigureAwait(false));

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		// -----------------------------------------------------------------------------------------
		// State-passing overloads
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task StatePassingOverloads_WorkForTaskAndValueTask()
		{
			var flights = new FlightGroup<string>();

			Task<string> taskState = flights.RunAsync(
				"state:task",
				42,
				static id => Task.FromResult($"user:{id}"));

			Assert.AreEqual(
				"user:42",
				await taskState.ConfigureAwait(false));

			Task<string> valueTaskState = flights.RunValueAsync(
				"state:value-task",
				42,
				static id => new ValueTask<string>($"user:{id}"));

			Assert.AreEqual(
				"user:42",
				await valueTaskState.ConfigureAwait(false));

			// Reference-type state as well.
			Task<int> referenceState = flights.RunAsync(
				"state:reference",
				"abcdef",
				static text => Task.FromResult(text.Length));

			Assert.AreEqual(
				6,
				await referenceState.ConfigureAwait(false));
		}


		// -----------------------------------------------------------------------------------------
		// Key behavior
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task CustomComparer_ControlsKeyEquality()
		{
			var flights = new FlightGroup<string>(
				StringComparer.OrdinalIgnoreCase);

			int executions = 0;

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> Load()
			{
				Interlocked.Increment(ref executions);
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return "case-insensitive";
			}

			Task<string> first = flights.RunAsync(
				"User:42",
				Load);

			await started.Task.ConfigureAwait(false);

			Task<string> second = flights.RunAsync(
				"USER:42",
				static () => Task.FromResult("ignored"));

			Assert.AreSame(
				first,
				second,
				"The custom comparer should make the two keys equivalent.");

			release.SetResult(true);

			Assert.AreEqual(
				"case-insensitive",
				await second.ConfigureAwait(false));

			Assert.AreEqual(
				1,
				executions);
		}


		[TestMethod]
		public async Task CompositeValueTypeKey_CoalescesByValue()
		{
			var flights =
				new FlightGroup<(int TenantId, int UserId)>();

			int executions = 0;

			var started = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<string> Load()
			{
				Interlocked.Increment(ref executions);
				started.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return "tenant1/user42";
			}

			Task<string> first = flights.RunAsync(
				(1, 42),
				Load);

			await started.Task.ConfigureAwait(false);

			Task<string> joiner = flights.RunAsync(
				(1, 42),
				static () => Task.FromResult("ignored"));

			Task<string> different = flights.RunAsync(
				(2, 42),
				static () => Task.FromResult("tenant2/user42"));

			Assert.AreSame(
				first,
				joiner);

			Assert.AreNotSame(
				first,
				different);

			release.SetResult(true);

			Assert.AreEqual(
				"tenant1/user42",
				await joiner.ConfigureAwait(false));

			Assert.AreEqual(
				"tenant2/user42",
				await different.ConfigureAwait(false));

			Assert.AreEqual(
				1,
				executions);
		}


		// -----------------------------------------------------------------------------------------
		// ExecutionContext
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		public async Task ExecutionContextFlow_IsSuppressedByDefault()
		{
			var tenant = new AsyncLocal<string?>
			{
				Value = "abc"
			};

			var flights =
				new FlightGroup<string>(
					suppressExecutionContextFlow: true);

			Task<string> task = flights.RunAsync(
				"context",
				async () =>
				{
					string? before = tenant.Value;

					await Task.Yield();

					return $"[{before}]/[{tenant.Value}]";
				});

			Assert.AreEqual(
				"[abc]/[]",
				await task.ConfigureAwait(false),
				"The synchronous prefix should see the caller context, but it should not flow across the await.");
		}


		[TestMethod]
		public async Task ExecutionContextFlow_CanBeEnabled()
		{
			var tenant = new AsyncLocal<string?>
			{
				Value = "abc"
			};

			var flights =
				new FlightGroup<string>(
					suppressExecutionContextFlow: false);

			Task<string> task = flights.RunAsync(
				"context",
				async () =>
				{
					string? before = tenant.Value;

					await Task.Yield();

					return $"[{before}]/[{tenant.Value}]";
				});

			Assert.AreEqual(
				"[abc]/[abc]",
				await task.ConfigureAwait(false));
		}


		// -----------------------------------------------------------------------------------------
		// Stress / concurrency
		// -----------------------------------------------------------------------------------------

		[TestMethod]
		[Timeout(30_000, CooperativeCancellation = true)]
		public async Task ManyKeysManyCallers_ExecuteOncePerKey()
		{
			const int KeyCount = 100;
			const int CallersPerKey = 50;
			const int TotalCallers = KeyCount * CallersPerKey;

			var flights = new FlightGroup<int>();

			int executions = 0;

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<int> Load(int key)
			{
				Interlocked.Increment(ref executions);

				await release.Task.ConfigureAwait(false);

				return key * 2;
			}

			var tasks = new Task<int>[TotalCallers];

			int index = 0;

			for (int key = 0; key < KeyCount; key++)
			{
				for (int caller = 0; caller < CallersPerKey; caller++)
				{
					int capturedKey = key;

					tasks[index++] = flights.RunAsync(
						capturedKey,
						() => Load(capturedKey));
				}
			}

			Assert.AreEqual(
				KeyCount,
				flights.InFlightCount,
				"Exactly one flight should exist for each distinct key.");

			Assert.AreEqual(
				KeyCount,
				executions,
				"Exactly one loader should have started for each key.");

			release.SetResult(true);

			int[] results = await Task.WhenAll(tasks)
				.ConfigureAwait(false);

			Assert.AreEqual(
				TotalCallers,
				results.Length);

			Assert.AreEqual(
				KeyCount,
				executions);

			Assert.IsTrue(
				results.All(static x => x % 2 == 0));

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		[TestMethod]
		[Timeout(30_000, CooperativeCancellation = true)]
		public async Task ThousandThreadPoolCallers_SameKey_ExecuteExactlyOnce()
		{
			const int CallerCount = 1000;

			var flights = new FlightGroup<string>();

			int executions = 0;
			int entered = 0;

			var loaderStarted = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var allEntered = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			async Task<int> Load()
			{
				Interlocked.Increment(ref executions);

				loaderStarted.TrySetResult(true);

				await release.Task.ConfigureAwait(false);

				return 314;
			}

			Task<int>[] callers = Enumerable.Range(
					0,
					CallerCount)
				.Select(
					_ => Task.Run(
						() =>
						{
							Task<int> task =
								flights.RunAsync(
									"thread-stress",
									Load);

							if (Interlocked.Increment(ref entered) == CallerCount)
							{
								allEntered.TrySetResult(true);
							}

							return task;
						}))
				.ToArray();

			await loaderStarted.Task.ConfigureAwait(false);

			// Do not allow the flight to complete until every caller has actually
			// executed RunAsync and therefore joined the same in-flight operation.
			await allEntered.Task.ConfigureAwait(false);

			Assert.AreEqual(
				1,
				flights.InFlightCount);

			Assert.AreEqual(
				1,
				executions);

			release.SetResult(true);

			int[] results = await Task.WhenAll(callers)
				.ConfigureAwait(false);

			Assert.AreEqual(
				1,
				executions,
				"All overlapping callers should share one loader execution.");

			Assert.IsTrue(
				results.All(static x => x == 314));

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		[TestMethod]
		[Timeout(30_000, CooperativeCancellation = true)]
		public async Task ConcurrentColdMisses_DifferentKeys_AllEstablishIndependentFlights()
		{
			const int KeyCount = 256;

			var flights = new FlightGroup<int>();

			int executions = 0;
			int loadersStarted = 0;

			var start = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var allLoadersStarted = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			var release = new TaskCompletionSource<bool>(
				TaskCreationOptions.RunContinuationsAsynchronously);

			Task<int>[] callers = Enumerable.Range(
					0,
					KeyCount)
				.Select(
					key => Task.Run(
						async () =>
						{
							await start.Task.ConfigureAwait(false);

							return await flights.RunAsync(
									key,
									async () =>
									{
										Interlocked.Increment(ref executions);

										if (Interlocked.Increment(ref loadersStarted) == KeyCount)
										{
											allLoadersStarted.TrySetResult(true);
										}

										await release.Task.ConfigureAwait(false);

										return key;
									})
								.ConfigureAwait(false);
						}))
				.ToArray();

			// Release all callers together so they contend on the hybrid miss path.
			start.SetResult(true);

			await allLoadersStarted.Task.ConfigureAwait(false);

			Assert.AreEqual(
				KeyCount,
				executions,
				"Every distinct key should establish exactly one loader.");

			Assert.AreEqual(
				KeyCount,
				flights.InFlightCount,
				"All distinct-key flights should remain registered while blocked.");

			release.SetResult(true);

			int[] results = await Task.WhenAll(callers)
				.ConfigureAwait(false);

			for (int i = 0; i < KeyCount; i++)
			{
				Assert.AreEqual(
					i,
					results[i]);
			}

			Assert.AreEqual(
				KeyCount,
				executions);

			Assert.AreEqual(
				0,
				flights.InFlightCount);
		}


		// -----------------------------------------------------------------------------------------
		// Helpers
		// -----------------------------------------------------------------------------------------

		private static async Task<TException> CaptureExceptionAsync<TException>(
			Task task)
			where TException : Exception
		{
			try
			{
				await task.ConfigureAwait(false);
			}
			catch (TException exception)
			{
				return exception;
			}
			catch (Exception exception)
			{
				Assert.Fail(
					$"Expected {typeof(TException).Name}, but received " +
					$"{exception.GetType().Name}: {exception.Message}");
			}

			Assert.Fail(
				$"Expected {typeof(TException).Name}, but the Task completed successfully.");

			throw new InvalidOperationException("Unreachable.");
		}


		private static void AssertThrowsSynchronously<TException>(
			Action action)
			where TException : Exception
		{
			try
			{
				action();
			}
			catch (TException)
			{
				return;
			}
			catch (Exception exception)
			{
				Assert.Fail(
					$"Expected synchronous {typeof(TException).Name}, but received " +
					$"{exception.GetType().Name}: {exception.Message}");
			}

			Assert.Fail(
				$"Expected synchronous {typeof(TException).Name}, but no exception was thrown.");
		}


		private static void UpdateMax(
			ref int location,
			int value)
		{
			while (true)
			{
				int current = Volatile.Read(ref location);

				if (value <= current)
				{
					return;
				}

				if (Interlocked.CompareExchange(
					ref location,
					value,
					current) == current)
				{
					return;
				}
			}
		}
	}
}