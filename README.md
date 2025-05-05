# Background

Lettuce uses Netty as its I/O library. Every time Lettuce dispatches a Redis command, it calls Channel#writeAndFlush.
Internally, writeAndFlush triggers a `writev` system call if underline eventloop is `epoll`, which causes frequent
context switches.
It also leads to TCP small-packet issues, wasting network bandwidth.

[See merge request for original context]

Optimization Approach
Merge I/O: auto-batch flush.

**Key challenge**: Although Lettuce is based on asynchronous I/O, most usages still share one connection across multiple
threads and call synchronously. We must guarantee thread safety.

## Optimization Details

### Command Buffering

When a client issues a command, we don’t send it directly to the channel. Instead, we buffer it into an MPSC (
multi-producer, single-consumer) task queue. We use JCTools’ MPSC queue (one of the fastest Java implementations, also
adopted by Netty).

### Batch Consumption

The user thread schedules the channel-bound event-loop thread to batch-consume from this task queue:

```java
private void scheduleSendJobOnConnected(final ContextualChannel chan) {
        // Schedule directly
        loopDrain(chan, false);
    }

    private void scheduleSendJobIfNeeded(final ContextualChannel chan) {
        final EventLoop eventLoop = chan.eventLoop();
        if (eventLoop.inEventLoop()) {
            // Possible in reactive() mode.
            loopDrain(chan, false);
            return;
        }

        if (chan.context.autoBatchFlushEndPointContext.hasOngoingSendLoop.tryEnter()) {
            // Benchmark result:
            // Redis:
            // engine: 7.1.0
            // server: AWS elasticcache cache.r7g.large
            // Client: EC2-c5n.2xlarge
            // Test Model:
            // multi-thread sync exists (./bench-multi-thread-exists.sh -b 32 -s 10 -n 80000 -t 64)
            // Test Parameter:
            // thread num: 64, loop num: 80000, batch size: 32, write spin count: 10
            //
            // With tryEnter():
            // Avg latency: 0.64917373203125ms
            // Avg QPS: 196037.67991971457/s
            // Without tryEnter():
            // Avg latency: 0.6618976359375001ms
            // Avg QPS: 192240.1301551348/s
            try {
                eventLoop.execute(() -> loopDrain(chan, true));
            } catch (Exception e) {
                logger.error("scheduleSendJobIfNeeded failed", e);
                chan.context.autoBatchFlushEndPointContext.hasOngoingSendLoop.exit();
                throw e;
            }
        }

        // Otherwise:
        // 1. offer() (volatile write of producerIndex) synchronizes-before hasOngoingSendLoop.safe.get() == 1 (volatile read)
        // 2. hasOngoingSendLoop.safe.get() == 1 (volatile read) synchronizes-before
        // hasOngoingSendLoop.safe.set(0) (volatile write) in first loopDrain0()
        // 3. hasOngoingSendLoop.safe.set(0) (volatile write) synchronizes-before
        // taskQueue.isEmpty() (volatile read of producerIndex), which guarantees to see the offered task.
    }

    private void loopDrain(final ContextualChannel chan, boolean entered) {
        final ConnectionContext connectionContext = chan.context;
        final AutoBatchFlushEndPointContext autoBatchFlushEndPointContext = connectionContext.autoBatchFlushEndPointContext;
        if (connectionContext.isChannelInactiveEventFired()
                || autoBatchFlushEndPointContext.hasRetryableFailedToSendCommands()) {
            return;
        }

        LettuceAssert.assertState(channel == chan, "unexpected: channel not match but closeStatus == null");
        loopDrain0(autoBatchFlushEndPointContext, chan, writeSpinCount, entered);
    }

    private void loopDrain0(final AutoBatchFlushEndPointContext autoBatchFlushEndPointContext, final ContextualChannel chan,
            int remainingSpinnCount, final boolean entered) {
        do {
            final int count = pollAndFlushInBatch(autoBatchFlushEndPointContext, chan);
            if (count == 0) {
                break;
            }
            if (count < 0) {
                return;
            }
        } while (--remainingSpinnCount > 0);

        if (remainingSpinnCount <= 0) {
            // Don't need to exitUnsafe since we still have an ongoing consume tasks in this thread.
            chan.eventLoop().execute(() -> loopDrain(chan, entered));
            return;
        }

        if (entered) {
            // queue was empty
            // The send loop will be triggered later when a new task is added,
            autoBatchFlushEndPointContext.hasOngoingSendLoop.exit();
            // Guarantee thread-safety: no dangling tasks in the queue, see scheduleSendJobIfNeeded()
            if (!taskQueue.isEmpty()) {
                loopDrain0(autoBatchFlushEndPointContext, chan, remainingSpinnCount, false);
            }
        }
    }

    private int pollAndFlushInBatch(final AutoBatchFlushEndPointContext autoBatchFlushEndPointContext, ContextualChannel chan) {
        int count = 0;
        while (count < batchSize) {
            if (debugEnabled) {
                taskQueueConsumeSync.assertIsOwnerThreadAndPreemptPrevented();
            }
            final Object o = this.taskQueue.poll();
            if (o == null) {
                break;
            }

            if (o instanceof RedisCommand<?, ?, ?>) {
                autoBatchFlushEndPointContext.add(1);
                RedisCommand<?, ?, ?> cmd = (RedisCommand<?, ?, ?>) o;
                channelWrite(chan, cmd).addListener(WrittenToChannel.newInstance(this, chan, cmd, false));
                count++;
            } else {
                @SuppressWarnings("unchecked")
                Collection<? extends RedisCommand<?, ?, ?>> commands = (Collection<? extends RedisCommand<?, ?, ?>>) o;
                final int commandsSize = commands.size(); // size() could be expensive for some collections so cache it!
                autoBatchFlushEndPointContext.add(commandsSize);
                for (RedisCommand<?, ?, ?> cmd : commands) {
                    channelWrite(chan, cmd).addListener(WrittenToChannel.newInstance(this, chan, cmd, false));
                }
                count += commandsSize;
            }
        }

        if (count > 0) {
            channelFlush(chan);
            if (autoBatchFlushEndPointContext.hasRetryableFailedToSendCommands()) {
                // Wait for onConnectionClose event()
                return -1;
            }
        }
        return count;
    }
```

### Failure Compensation and Order Preservation
Lettuce’s advantage is “connection unawareness”: on disconnect it auto-reconnects and retries failed commands so that
the application needn‘t care about connection state. By default, upon reconnect it replays:

1. Commands written successfully but without a response.
2. Commands whose write failed.

Our enhancement maintains two separate queues:

1. pendingCommands for (1)
2. failedRetryableCommands for (2)

At a quiescent point (defined below) we efficiently compensate by prepending all entries back into the MPSC queue:

```java
public class JcToolsUnboundedMpscOfferFirstQueue<E> implements UnboundedOfferFirstQueue<E> {

    private final LinkedList<Queue<? extends E>> unsafeQueues = new LinkedList<>();
    private final Queue<E> mpscQueue = PlatformDependent.newMpscQueue();

    @Override
    public void offer(E e) {
        mpscQueue.offer(e);
    }

    /**
     * Must be called by consumer thread.
     * @param q a queue whose contents to prepend
     */
    @Override
    public void offerFirstAll(@Nullable Deque<? extends E> q) {
        if (q != null && !q.isEmpty()) {
            unsafeQueues.addFirst(q);
        }
    }

    @Override
    public E poll() {
        if (!unsafeQueues.isEmpty()) {
            return pollFromUnsafeQueues();
        }
        return mpscQueue.poll();
    }

    private E pollFromUnsafeQueues() {
        Queue<? extends E> first = unsafeQueues.getFirst();
        E e = first.poll();
        if (first.isEmpty()) {
            unsafeQueues.removeFirst();
        }
        return Objects.requireNonNull(e);
    }

}
```
### Consumer Thread Migration
A quiescent point is when the system is “still” or stable—no inflight activity that could cause inconsistency. Here it
means:

1. channelInactive event has fired, and
2. All write-listeners have returned.

Because reconnection may bind the channel to a different Netty event-loop thread (Netty does round-robin), we need to
migrate our single-consumer safely. We use a volatile flag to establish a JMM happens-before edge:

At quiescence and after compensation, write to a volatile field in old thread.

In new thread, read that volatile before consuming.

Thus all previous writes become visible to the new consumer thread.

Key callbacks and events (simplified):

```java

    // write callback
    public void operationComplete(Future<Void> future) {
        try {
        ...
            QUEUE_SIZE.decrementAndGet(endpoint);
            bfCtx.done(1);
        
            Throwable retryErr = checkSendResult(future);
            if (retryErr != null && bfCtx.addRetryableFailedToSendCommand(cmd, retryErr)) {
                internalCloseConnectionIfNeeded(retryErr);
            }
        
            endpoint.trySetEndpointQuiescence(chan);
        } finally {
            recycle();
        }
    }

    // on channel inactive
    @Override
    public void notifyChannelInactiveAfterWatchdogDecision(Channel channel,
            Deque<RedisCommand<?, ?, ?>> retryablePendingCommands) {
        final ContextualChannel inactiveChan = this.channel;
        if (!inactiveChan.context.initialState.isConnected()) {
            logger.error("[unexpected][{}] notifyChannelInactive: channel initial state not connected", logPrefix());
            onUnexpectedState("notifyChannelInactiveAfterWatchdogDecision", ConnectionContext.State.CONNECTED);
            return;
        }

        if (inactiveChan.getDelegate() != channel) {
            logger.error("[unexpected][{}] notifyChannelInactive: channel not match", logPrefix());
            onUnexpectedState("notifyChannelInactiveAfterWatchdogDecision: channel not match",
                    ConnectionContext.State.CONNECTED);
            return;
        }

        if (inactiveChan.context.isChannelInactiveEventFired()) {
            logger.error("[unexpected][{}] notifyChannelInactive: already fired", logPrefix());
            return;
        }

        boolean willReconnect = connectionWatchdog != null
                && connectionWatchdog.willReconnectOnAutoBatchFlushEndpointQuiescence();
        RedisException exception = null;
        // Unlike DefaultEndpoint, here we don't check reliability since connectionWatchdog.willReconnect() already does it.
        if (isClosed()) {
            exception = new RedisException("endpoint closed");
            willReconnect = false;
        }

        if (willReconnect) {
            this.logPrefix = null;
            CHANNEL.set(this, DummyContextualChannelInstances.CHANNEL_WILL_RECONNECT);
            // Create a synchronize-before with this.channel = CHANNEL_WILL_RECONNECT
            if (isClosed()) {
                exception = new RedisException("endpoint closed");
                willReconnect = false;
            } else {
                exception = new RedisException("channel inactive and will reconnect");
            }
        } else if (exception == null) {
            exception = new RedisException("channel inactive and connectionWatchdog won't reconnect");
        }

        if (!willReconnect) {
            this.logPrefix = null;
            CHANNEL.set(this, DummyContextualChannelInstances.CHANNEL_ENDPOINT_CLOSED);
        }
        inactiveChan.context
                .setCloseStatus(new ConnectionContext.CloseStatus(willReconnect, retryablePendingCommands, exception));
        trySetEndpointQuiescence(inactiveChan);
    }

    private void trySetEndpointQuiescence(ContextualChannel chan) {
        final EventLoop eventLoop = chan.eventLoop();
        LettuceAssert.isTrue(eventLoop.inEventLoop(), "unexpected: not in event loop");

        final ConnectionContext connectionContext = chan.context;
        final @Nullable ConnectionContext.CloseStatus closeStatus = connectionContext.getCloseStatus();
        if (closeStatus == null) {
            return;
        }

        final AutoBatchFlushEndPointContext autoBatchFlushEndPointContext = connectionContext.autoBatchFlushEndPointContext;
        if (!autoBatchFlushEndPointContext.isDone()) {
            return;
        }

        if (closeStatus.isWillReconnect()) {
            onWillReconnect(closeStatus, autoBatchFlushEndPointContext);
        } else {
            onWontReconnect(closeStatus, autoBatchFlushEndPointContext);
        }

        if (chan.context.setChannelQuiescentOnce()) {
            onEndpointQuiescence();
        } else {
            ExceptionUtils.maybeFire(logger, canFire, "unexpected: quiescence already acquired");
        }
    }

    private void onEndpointQuiescence() {
        taskQueueConsumeSync.done(1); // allows preemption

        if (channel.context.initialState == ConnectionContext.State.ENDPOINT_CLOSED) {
        return;
        }

        this.logPrefix = null;
        // Create happens-before with channelActive()
        if (!CHANNEL.compareAndSet(this, DummyContextualChannelInstances.CHANNEL_WILL_RECONNECT,
        DummyContextualChannelInstances.CHANNEL_CONNECTING)) {
        onUnexpectedState("onEndpointQuiescence", ConnectionContext.State.WILL_RECONNECT);
        return;
        }

        // notify connectionWatchDog that it is safe to reconnect now.
        // neither connectionWatchdog nor doReconnectOnEndpointQuiescence could be null
        // noinspection DataFlowIssue
        connectionWatchdog.reconnectOnAutoBatchFlushEndpointQuiescence();
    }
```
### Closing the Endpoint
On close we must cancel all pending commands. In the original implementation, internal data structures are thread-safe
and can be cleared directly. With our MPSC queue, cancellations must be deferred to the event-loop:

During state-machine stepping, check close state.

At quiescent point, asynchronously clear taskQueue.

### Preventing Dangling Commands
To avoid commands stuck forever in the MPSC queue if no consumer runs, we do a double-check around enqueue:

### Additional Optimization: Single-flight style Dispatching
Every dispatch currently calls `scheduleSendJobIfNeeded`, flooding the event-loop with closures. We introduce an
optimistic lock (hasOngoingSendLoop) to reduce eventLoop.execute(...) calls:

```java
public class DefaultAutoBatchFlushEndpoint implements RedisChannelWriter, AutoBatchFlushEndpoint, PushHandler {
    private void scheduleSendJobIfNeeded(final ContextualChannel chan) {
        final EventLoop eventLoop = chan.eventLoop();
        if (eventLoop.inEventLoop()) {
            // Possible in reactive() mode.
            loopDrain(chan, false);
            return;
        }

        if (chan.context.autoBatchFlushEndPointContext.hasOngoingSendLoop.tryEnter()) {
            // Benchmark result:
            // Redis:
            // engine: 7.1.0
            // server: AWS elasticcache cache.r7g.large
            // Client: EC2-c5n.2xlarge
            // Test Model:
            // multi-thread sync exists (./bench-multi-thread-exists.sh -b 32 -s 10 -n 80000 -t 64)
            // Test Parameter:
            // thread num: 64, loop num: 80000, batch size: 32, write spin count: 10
            //
            // With tryEnter():
            // Avg latency: 0.64917373203125ms
            // Avg QPS: 196037.67991971457/s
            // Without tryEnter():
            // Avg latency: 0.6618976359375001ms
            // Avg QPS: 192240.1301551348/s
            try {
                eventLoop.execute(() -> loopDrain(chan, true));
            } catch (Exception e) {
                logger.error("scheduleSendJobIfNeeded failed", e);
                chan.context.autoBatchFlushEndPointContext.hasOngoingSendLoop.exit();
                throw e;
            }
        }

        // Otherwise:
        // 1. offer() (volatile write of producerIndex) synchronizes-before hasOngoingSendLoop.safe.get() == 1 (volatile read)
        // 2. hasOngoingSendLoop.safe.get() == 1 (volatile read) synchronizes-before
        // hasOngoingSendLoop.safe.set(0) (volatile write) in first loopDrain0()
        // 3. hasOngoingSendLoop.safe.set(0) (volatile write) synchronizes-before
        // taskQueue.isEmpty() (volatile read of producerIndex), which guarantees to see the offered task.
    }

    private void loopDrain(final ContextualChannel chan, boolean entered) {
        final ConnectionContext connectionContext = chan.context;
        final AutoBatchFlushEndPointContext autoBatchFlushEndPointContext = connectionContext.autoBatchFlushEndPointContext;
        if (connectionContext.isChannelInactiveEventFired()
                || autoBatchFlushEndPointContext.hasRetryableFailedToSendCommands()) {
            return;
        }

        LettuceAssert.assertState(channel == chan, "unexpected: channel not match but closeStatus == null");
        loopDrain0(autoBatchFlushEndPointContext, chan, writeSpinCount, entered);
    }

    private void loopDrain0(final AutoBatchFlushEndPointContext autoBatchFlushEndPointContext, final ContextualChannel chan,
            int remainingSpinnCount, final boolean entered) {
        do {
            final int count = pollAndFlushInBatch(autoBatchFlushEndPointContext, chan);
            if (count == 0) {
                break;
            }
            if (count < 0) {
                return;
            }
        } while (--remainingSpinnCount > 0);

        if (remainingSpinnCount <= 0) {
            // Don't need to exitUnsafe since we still have an ongoing consume tasks in this thread.
            chan.eventLoop().execute(() -> loopDrain(chan, entered));
            return;
        }

        if (entered) {
            // queue was empty
            // The send loop will be triggered later when a new task is added,
            autoBatchFlushEndPointContext.hasOngoingSendLoop.exit();
            // Guarantee thread-safety: no dangling tasks in the queue, see scheduleSendJobIfNeeded()
            if (!taskQueue.isEmpty()) {
                loopDrain0(autoBatchFlushEndPointContext, chan, remainingSpinnCount, false);
            }
        }
    }

}
```

# Benchmark 
macOS Monterey, M1 Pro, Redis 7.0.11:

## Scenario QPS Latency
### Single-thread async-get		
|                    |    QPS    | Latency |
|--------------------|-----------|--------|
| enable_auto_batch  | 507,148.4 | 3.05 s | 
| disable_auto_batch | 151,769.3 | 26.9 s |
### Multi-thread sync-get (32 th)		
|                    |    QPS    | Latency  |
|--------------------|-----------|--------|
| enable_auto_batch  | 182,125.9 | 173.8 μs |
| disable_auto_batch | 108,553.5 | 292.6 μs |

## Bandwidth Savings
Assuming a 13 B key and 15 B value in GET:

Without batching (payload ≈ 30 B):

Ethernet header: 14 B

IPv4 header: 20 B

TCP header: 20 B

Total frame: 84 B

Payload ratio ≈ 30 / 84 ≈ 35.7%

With batch of 8 (payload ≈ 240 B):

Payload ratio ≈ 240 / (14+20+20+240) ≈ 81.6%

Thus, batching cuts protocol overhead by more than half (even more with larger batches, at the cost of latency).

## Summary
50% latency decreasement compared to original mode.
Performance is comparable to `ConsolidateFlushHandler` (sometimes better) but doesn't have the widely criticized high latency overhead in low traffic scenarios of `ConsolidateFlushHandler`. This is because kale won't wait a full batch to flush, instead it will flush as soon as possible in case of low traffic.
