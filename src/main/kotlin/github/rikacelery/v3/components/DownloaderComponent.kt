package github.rikacelery.v3.components

import github.rikacelery.v3.core.Actor
import github.rikacelery.v3.core.DataChannel
import github.rikacelery.v3.core.EventBus
import github.rikacelery.v3.core.OrderedEmitter
import github.rikacelery.v3.data.DownloadMeta
import github.rikacelery.v3.data.DownloadResult
import github.rikacelery.v3.events.*
import github.rikacelery.v3.hooks.DownloaderHook
import github.rikacelery.v3.utils.ClientManager
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.utils.io.*
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds

sealed interface DownloaderMsg
data class DoDownload(val cmd: Download) : DownloaderMsg
data class DoCutPoint(val cut: CutPoint) : DownloaderMsg

data class ActiveDownload(
    val emitter: OrderedEmitter,
    val semaphore: Semaphore,
    val runningJobs: MutableSet<Job>,
    var idx: AtomicInteger = AtomicInteger(-1),
    var generation: Int = 0,
    var active: Boolean = true
)

class DownloaderComponent(
    private val dataChannel: DataChannel,
    private val hooks: List<DownloaderHook> = emptyList(),
    eventBus: EventBus,
    parentScope: CoroutineScope,
    private val initialConcurrency: Int = 16
) : Actor<DownloaderMsg>("DownloaderComponent", eventBus, parentScope) {

    private val rooms = ConcurrentHashMap<Long, ActiveDownload>()
    private val workerScope = CoroutineScope(
        parentScope.coroutineContext + SupervisorJob() + CoroutineName("downloader-worker")
    )

    override suspend fun handle(msg: DownloaderMsg) {
        if (!scope.isActive) return
        when (msg) {
            is DoDownload -> handleDownload(msg.cmd)
            is DoCutPoint -> handleCutPoint(msg.cut)

        }
    }

    private suspend fun handleDownload(cmd: Download) {
        val active = rooms.getOrPut(cmd.roomId) {
            ActiveDownload(
                emitter = OrderedEmitter(cmd.roomId) { dataChannel.send(it) },
                semaphore = Semaphore(initialConcurrency),
                runningJobs = mutableSetOf()
            )
        }
        if (!active.active) return
        active.generation = cmd.generation

        for (seg in cmd.urls) {
            val idx = active.idx.incrementAndGet()
            var url = seg.url
            hooks.forEach { url = it.beforeDownload(url) }

            val job = workerScope.launch {
                active.semaphore.withPermit {
                    eventBus.publish(DownloadStarted(cmd.roomId, idx, url, System.currentTimeMillis()))
                    val result = downloadSegment(url, idx)
                    val hooked = hooks.fold(result) { acc, hook -> hook.onDownloadResult(cmd.roomId, acc) }
                    active.emitter.complete(idx.toLong(), hooked)

                    when (result) {
                        is DownloadResult.Success -> {
                            eventBus.publish(SegmentDownloaded(cmd.roomId, idx, seg.url,
                                result.meta.fetchDurationMs, result.meta.proxied, result.data.size, active.generation))
                        }
                        is DownloadResult.Failed -> {
                            eventBus.publish(DownloadError(cmd.roomId, idx, seg.url, result.reason))
                        }
                        is DownloadResult.CutPoint -> {}
                    }
                }
            }
            active.runningJobs.add(job)
            job.invokeOnCompletion { active.runningJobs.remove(job) }
        }
    }

    private suspend fun handleCutPoint(cut: CutPoint) {
        val active = rooms[cut.roomId] ?: return
        val idx = active.idx.incrementAndGet().toLong()
        logger.info("CutPoint roomId={}, index={}, reason={}", cut.roomId, cut.index, cut.reason)
        CoroutineScope(Dispatchers.IO).launch {
            try {
                active.emitter.complete(idx, DownloadResult.CutPoint(cut))
            } catch (e: Exception) {
                logger.error("Failed to complete cut point for roomId={}", cut.roomId, e)
            }
        }
    }

    private val raceThresholdMs: Long = 15_000

    private suspend fun downloadSegment(url: String, idx: Int): DownloadResult {
        val start = System.currentTimeMillis()

        return try {
            val directDeferred = scope.async {
                downloadWithClient(ClientManager.getClient("dl_${Random.nextInt(32)}"), url, idx, false)
            }

            val directResult = withTimeoutOrNull(raceThresholdMs.milliseconds) { directDeferred.await() }
            if (directResult is DownloadResult.Success) {
                val dur = System.currentTimeMillis() - start
                return directResult.copy(meta = directResult.meta.copy(fetchDurationMs = dur, proxied = false))
            }

            logger.debug("Direct download slow/failed for idx={}, falling back to proxy race", idx)
            // Phase 2: proxy joins the race
            val proxyDeferred = scope.async {
                downloadWithClient(ClientManager.getProxiedClient("px_${Random.nextInt(5)}"), url, idx, true)
            }

            val result = select<DownloadResult> {
                directDeferred.onAwait { r ->
                    (r as? DownloadResult.Success)?.copy(meta = r.meta.copy(
                        fetchDurationMs = System.currentTimeMillis() - start, proxied = false)) ?: r
                }
                proxyDeferred.onAwait { r ->
                    (r as? DownloadResult.Success)?.copy(meta = r.meta.copy(
                        fetchDurationMs = System.currentTimeMillis() - start, proxied = true)) ?: r
                }
            }

            if (!directDeferred.isCompleted) directDeferred.cancel()
            if (!proxyDeferred.isCompleted) proxyDeferred.cancel()
            result
        } catch (e: Exception) {
            logger.error("downloadSegment failed: idx=$idx, url=$url", e)
            DownloadResult.Failed(idx, url, e.message ?: "download failed")
        }
    }

    private suspend fun downloadWithClient(
        client: HttpClient, url: String, idx: Int, proxied: Boolean
    ): DownloadResult {
        return try {
            val response = client.get(url)
            val stream = response.bodyAsChannel()
            val bos = ByteArrayOutputStream()
            while (!stream.isClosedForRead) {
                val buf = ByteArray(8192)
                val read = stream.readAvailable(buf)
                if (read <= 0) break
                bos.write(buf, 0, read)
            }
            DownloadResult.Success(bos.toByteArray(), DownloadMeta(url, 0, proxied, Instant.now()))
        } catch (e: Exception) {
            logger.error("downloadWithClient failed: idx=$idx, url=$url, proxied=$proxied", e)
            DownloadResult.Failed(idx, url, e.message ?: "download failed")
        }
    }
}