# Refactor Plan: Non-blocking Event Bus

## Problem Statement

The event bus is blocked by synchronous I/O, causing a cascading failure. Specific symptoms:

1. **PostProcessorComponent shared mailbox blocking**: Large file copying by MoveProcessor (`input.copyTo(dest)`) executes synchronously in an Actor coroutine, causing file processing in one room to block subsequent messages in all rooms.
2. **ConfigComponent file I/O blocking the event pump**: `saveConfig()` executes `configFile.writeText()` in the collector coroutine of the EventBus subscriber. When the disk is slow, the entire ConfigComponent stops responding, causing all `GetDecryptKey` / `MatchDecryptKeys` requests to timeout (in the logs, 25 sessions all timeout within the same millisecond).
3. **pollingLoop lacks staggering**: Multiple sessions have synchronous 3-second polling cycles, triggering many identical requests at the same time, worsening congestion.
4. **Final crash**: Event backlog + file handle leaks → `OutOfMemoryError: Java heap space`.
5. **No test coverage**: Core concurrency frameworks (EventBus, Actor, RequestBus) have no automated tests.

## Solution

Refactor in five modules. Core principles: **isolate by room, async blocking I/O, event pump never blocks, backlog observable**.

### Module 1: Core framework testing (EventBus / Actor / RequestBus / DataChannel / OrderedEmitter)

Write tests using `kotlinx-coroutines-test` + JUnit5 `runTest`. OrderedEmitter uses mock DataChannel.

### Module 2: Event bus backlog monitoring (EventBus / Actor)

**EventBus**: `publish()` first uses `tryEmit()`. Returning false means SharedFlow buffer full and events are backing up. Record the type and timestamp of the first backlog event; if backlog continues, summarize and print types and counts at intervals (every 30 seconds). Then fallback to `emit()` (suspend and wait, no event loss).

**Actor**: Record time before and after `handle()`. If it takes over 500ms, print WARN (including Actor name, message type, time in ms).

### Module 2: PostProcessorComponent dispatch by room

Maintain a global `Semaphore(4)` to limit concurrency, add per-room `Channel<FileReady>` + dedicated coroutine. `handle()` only routes to room Channel, using `scope.launch` and `channel.send()` to avoid blocking the Actor main loop. When a room stops, cleaning is driven by `RecordingStopped` event.

### Module 3: ConfigComponent non-blocking + SessionComponent cache

`loadConfig()`/`saveConfig()` file I/O shifted to `Dispatchers.IO`. `PersistConfig` processing changed to `scope.launch(Dispatchers.IO) { saveConfig() }` for full async. SessionComponent adds `ConcurrentHashMap` cache for decrypt key query results, cleared on `PersistConfig`.

### Module 4: pollingLoop staggering

Add random ±500ms jitter on the base 3-second delay for each pollingLoop start.

## Commits

### Commit 1: Add kotlinx-coroutines-test dependency

Add `testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")` in `build.gradle.kts`.

### Commit 2: EventBus tests

`src/test/kotlin/github/rikacelery/v3/core/EventBusTest.kt`

- `publish to single subscriber delivers event correctly`
- `publish to multiple subscribers each gets independent copy`
- `buffer full with DROP_OLDEST drops oldest events`
- `hook returns null swallows event`
- `concurrent publish from multiple coroutines delivers all events`

### Commit 3: Actor tests

`src/test/kotlin/github/rikacelery/v3/core/ActorTest.kt`

Create a test `TestActor` subclass. Cover:
- `tell order equals handle order` — sequencing guarantee
- `handle throws non-CancellationException triggers onError` — exception isolation
- `start then stop closes mailbox and cancels coroutine` — lifecycle
- `wrapEvent routes events into mailbox` — integration pipeline

### Commit 4: RequestBus tests

`src/test/kotlin/github/rikacelery/v3/core/RequestBusTest.kt`

- `request-response round-trip with CommandAck`
- `timeout throws RequestTimeoutException and cleans pending`
- `ErrorResponse throws RequestErrorException`
- `concurrent 100 requests each gets correct independent response`
- `parent coroutine cancelled cleans up CompletableDeferred`
- `CommandAck arrives before await (extreme out-of-order)`

### Commit 5: DataChannel tests

`src/test/kotlin/github/rikacelery/v3/core/DataChannelTest.kt`

- `send-receive preserves order`
- `hook returns null drops message`

### Commit 6: OrderedEmitter tests

`src/test/kotlin/github/rikacelery/v3/core/OrderedEmitterTest.kt`

Mock DataChannel, validate OrderedEmitter ordering logic only:
- `in-order completion emits in-order`
- `out-of-order completion emits sorted by seq`
- `CutPoint result triggers StreamEnd and resets emitter`

### Commit 7: ConfigComponent non-blocking IO

`ConfigComponent.kt`:

- Wrap `configFile.readText()` in `loadConfig()` with `withContext(Dispatchers.IO)`
- Wrap `configFile.writeText()` in `saveConfig()` with `withContext(Dispatchers.IO)`
- Change `PersistConfig` from calling `saveConfig()` directly to `scope.launch(Dispatchers.IO) { saveConfig() }`

### Commit 8: ConfigComponent tests

`src/test/kotlin/github/rikacelery/v3/components/ConfigComponentTest.kt`

- `GetDecryptKey found returns key value`
- `GetDecryptKey not found returns ConfigResponse(null)`
- `MatchDecryptKeys finds first matching key`
- `MatchDecryptKeys no match returns empty DecryptKeyMatch`
- `ToggleMask flips value and publishes PersistConfig`
- `saveConfig then loadConfig round-trips correctly`
- `saveConfig does not block GetDecryptKey request handling` — **core isolation test**

### Commit 9: SessionComponent decryptKeyCache

`SessionComponent.kt`:

- Add `private val decryptKeyCache = ConcurrentHashMap<String, String>()`
- In `pollingLoop` line 524: first check cache, on miss call `requestBus.request<ConfigResponse>(GetDecryptKey(rs.pkey))` and store result in cache
- Add `onPersistConfig()` handler to clear cache
- Subscribe to `PersistConfig` in `onStart()`

### Commit 10: PostProcessorComponent per-room dispatch

Rewrite `PostProcessorComponent.kt`:

- Add internal `RoomProcessor`: `Channel<FileReady>(capacity=8)` + dedicated `Job`
- Add `private val rooms = ConcurrentHashMap<Long, RoomProcessor>()`
- In `handle()` on `OnProcessorEvent(FileReady)` → `rooms.getOrPut(roomId)` → `scope.launch { room.channel.send(event) }`, no blocking on main loop
- Inside `RoomProcessor` coroutine: `for (event in channel) { semaphore.withPermit { processFile(event) } }`, global Semaphore retained

### Commit 11: PostProcessorComponent event-driven cleanup

- Subscribe to `RecordingStopped` in `onStart()`
- On `RecordingStopped`, wait for RoomProcessor's channel to empty, close channel, and remove from `rooms`

### Commit 12: PostProcessorComponent tests

`src/test/kotlin/github/rikacelery/v3/components/PostProcessorComponentTest.kt`

- `single room single FileReady routes to processFile`
- `single room multiple FileReady processed in arrival order` — same-room sequencing
- `room A slow processing does not block room B` — **core isolation test**
- `room A FileReady queued while room A still processing` — same-room serialization
- `RecordingStopped cleans up idle RoomProcessor`
- `new FileReady after cleanup recreates RoomProcessor`

### Commit 13: pollingLoop startup jitter

`SessionComponent.kt` `pollingLoop` method:

- First `delay(3.seconds)` changed to `delay(3.seconds + Random.nextLong(-500, 500).milliseconds)`
- Subsequent cycles keep standard 3-second interval (no jitter)

### Commit 14: EventBus backlog monitoring

`EventBus.kt`:

- In `publish()`, change `_events.emit(e!!)` to first call `tryEmit()`. If true, return immediately; if false, record backlog.
- Add backlog state fields: `backlogStartTime: Long` (timestamp of first backlog), `backlogCount: Long` (accumulated number), `firstBackloggedEvent: String` (event type triggering backlog).
- On `tryEmit()` false and `backlogStartTime == 0` (first backlog), print WARN: event type + "event bus buffer full, starting to back up".
- Subsequent false `tryEmit()` calls increment counter.
- If backlog exceeds 30 seconds or 1000 events, print summarized WARN of backlog duration, event count, and top N backlog event types, then reset counters.
- On `tryEmit()` true after backlog, print INFO: backlog cleared, total duration and count.
- Regardless of `tryEmit()` success, fallback to `_events.emit(e!!)` to avoid event loss.

### Commit 15: Actor slow handler monitoring

`Actor.kt`:

- Record `System.nanoTime()` before and after `handle(msg)`.
- If duration exceeds `slowHandlerThresholdMs` (default 500ms), print WARN: "slow handler: actor=$name, msg=${msg::class.simpleName}, took=${duration}ms".
- `slowHandlerThresholdMs` is an Actor constructor parameter, default 500ms, can be overridden by subclasses.

## Decision Document

### Module division

- **Core framework modifications + tests**: EventBus (tryEmit backlog monitoring), Actor (slow handler monitoring + tests), RequestBus (tests), DataChannel (tests), OrderedEmitter (tests)
- **PostProcessorComponent**: from global sequential Actor to per roomId dispatch
- **ConfigComponent**: async file I/O, fully async PersistConfig
- **SessionComponent**: add decryptKeyCache, pollingLoop jitter
- **No change**: WriterComponent (already uses DataChannel + IO dispatch), DownloaderComponent (handle only launches coroutine), SchedulerComponent (pure message routing)

### PostProcessorComponent design decisions

- **Keep global Semaphore**: to control ffmpeg process count globally; Semaphore acquired inside `processFile` coroutine via `withPermit`
- **Per-room Channel capacity 8**: queue up to 8 FileReady events per room; beyond that `send()` suspends (inside separate coroutine launched by `scope.launch`, does not block Actor main loop)
- **Cleanup strategy**: event-driven by `RecordingStopped`, wait for channel empty then close
- **handle() routing**: use `scope.launch { channel.send() }` not `trySend()`, ensuring no event loss

### ConfigComponent design decisions

- **loadConfig/saveConfig**: wrap file ops with `withContext(Dispatchers.IO)`, suspend but non-blocking thread
- **PersistConfig**: `scope.launch(Dispatchers.IO)` fully async, collector coroutine returns immediately
- **decryptKeyCache**: `ConcurrentHashMap<String, String>`, keys are pkeys, cleared on `PersistConfig`

### EventBus backlog monitoring design decisions

- **Two-layer mechanism**: quick `tryEmit()` detects buffer full, fallback `emit()` ensures no event loss. SharedFlow’s built-in DROP_OLDEST disabled via this approach.
- **Aggregate logging**: initial backlog logs WARN immediately; subsequent logs summarize every 30 seconds or after 1000 events, avoiding log flooding.
- **Clear notification**: print INFO when backlog clears, helpful for diagnosing congestion duration.
- **Keep DROP_OLDEST unchanged**: `extraBufferCapacity` and `BufferOverflow` remain as is. `tryEmit()` false signals internal buffer full, which is caught.

### Actor slow handler monitoring design decisions

- **Default threshold 500ms**: captures most reasonable durations; longer durations considered anomalies. Subclass can override.
- **Use `nanoTime` instead of `currentTimeMillis`**: monotonic clock, unaffected by system time changes.
- **No accumulation stats**: each slow handler logged individually; no internal stats maintained. Aggregation handled externally by log system.

### Jitter design decisions

- **Only add jitter on first delay**: subsequent cycles keep standard 3 seconds, avoids cumulative drift.
- **±500ms jitter range**: sufficiently spreads session start times, without impacting recording real-time requirements.

### Interface changes

- **EventBus.publish()**: behavior unchanged (suspend, no event loss), internally tryEmit + fallback emit + backlog logging
- **Actor constructor**: new parameter `slowHandlerThresholdMs: Long = 500`
- PostProcessorComponent subscribes to `RecordingStopped`
- SessionComponent subscribes to `PersistConfig`
- No external API changes

## Testing Decisions

### Test framework

- **kotlinx-coroutines-test** + `runTest`: virtual time, quickly test timeouts and concurrency
- **JUnit5** (configured): `assertThrows`, `@Test`
- **OrderedEmitter’s DataChannel**: mocked (manually implemented fake to record send messages)

### What counts as good tests

- Only test externally observable behaviors (published events, thrown exceptions, handling order), not internal implementation details
- Concurrency tests use `runTest` + `launch` multiple coroutines, verifying end states, not execution paths
- Isolation tests verifying "A blocking does not affect B" are core for each module

### Which modules are tested

| Module | Test file |
|--------|-----------|
| EventBus | `EventBusTest.kt` |
| Actor | `ActorTest.kt` (requires TestActor subclass) |
| RequestBus | `RequestBusTest.kt` |
| DataChannel | `DataChannelTest.kt` |
| OrderedEmitter | `OrderedEmitterTest.kt` (mock DataChannel) |
| ConfigComponent | `ConfigComponentTest.kt` |
| PostProcessorComponent | `PostProcessorComponentTest.kt` |

### Existing test facilities

- No test files currently
- `build.gradle.kts` includes `kotlin-test` + JUnit5 + `useJUnitPlatform()`
- Need to add `kotlinx-coroutines-test` dependency

## Out of Scope

- **RequestBus framework-level automatic deduplication**: not done; caching logic placed in SessionComponent business layer
- **EventBus BUFFER SIZE or OVERFLOW policy changes**: keep DROP_OLDEST + capacity=1024; backlog monitoring reports at tryEmit layer, no change in SharedFlow internals
- **Actor framework generic per-key dispatch**: not abstracted to Actor base class, implemented internally only in PostProcessorComponent
- **DownloaderComponent / WriterComponent refactor**: already use IO dispatch or per-room coroutines, no changes needed
- **SessionComponent handle() refactor**: analysis shows no blocking operations, no change needed
- **Automatic dump/persist of backlog events**: only log output, no serialization or persistence of events