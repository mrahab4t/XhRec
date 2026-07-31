package github.rikacelery.v3.components

import github.rikacelery.v3.core.Actor
import github.rikacelery.v3.core.DataChannel
import github.rikacelery.v3.core.EventBus
import github.rikacelery.v3.data.StreamData
import github.rikacelery.v3.data.StreamEnd
import github.rikacelery.v3.data.StreamEvent
import github.rikacelery.v3.data.StreamStart
import github.rikacelery.v3.events.EndReason
import github.rikacelery.v3.events.FileReady
import github.rikacelery.v3.events.WriterFatal
import github.rikacelery.v3.hooks.WriterHook
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap

sealed interface WriterMsg

data class ActiveFile(
    val file: File,
    val eventFile: File,
    val fos: FileOutputStream,
    val eventFos: FileOutputStream,
    val roomId: Long,
    val roomName: String,
    val startTime: Instant,
    val quality: String,
    var bytesWritten: Long = 0
) {
    companion object {
        private val log = LoggerFactory.getLogger(ActiveFile::class.java)
    }

    fun dispose() {
        try {
            fos.close()
        } catch (e: Exception) {
            log.error("Failed to close fos for room $roomId: ${e.message}", e)
        }
        try {
            eventFos.close()
        } catch (e: Exception) {
            log.error("Failed to close eventFos for room $roomId: ${e.message}", e)
        }
        file.delete()
        eventFile.delete()
    }
}

class WriterComponent(
    private val dataChannel: DataChannel,
    private val tmpDir: File,
    private val hooks: List<WriterHook> = emptyList(),
    eventBus: EventBus,
    private val parentScope: CoroutineScope
) : Actor<WriterMsg>("WriterComponent", eventBus, parentScope) {

    private val files = ConcurrentHashMap<Long, ActiveFile>()
    private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss")
        .withZone(ZoneId.systemDefault())

    override suspend fun onStart(scope: CoroutineScope) {
        scope.launch {
            while (isActive) {
                when (val msg = dataChannel.receive()) {
                    is StreamStart -> handleStreamStart(msg)
                    is StreamData -> handleStreamData(msg)
                    is StreamEnd -> handleStreamEnd(msg)
                    is StreamEvent -> handleStreamEvent(msg)
                }
            }
        }
    }

    override suspend fun handle(msg: WriterMsg) {}

    private suspend fun handleStreamStart(msg: StreamStart) {
        val existing = files.remove(msg.roomId)
        if (existing != null) {
            logger.info("Duplicate StreamStart for room ${msg.roomId}, closing existing file")
            parentScope.launch(Dispatchers.IO + NonCancellable) {
                closeActiveFile(existing, EndReason.NewInit)
            }
        }

        val timestamp = timeFormatter.format(msg.startTime)
        var path = "${tmpDir.absolutePath}/${msg.roomName}-$timestamp-init.mp4"

        try {
            withContext(Dispatchers.IO) {
                hooks.forEach { path = it.beforeFileOpen(msg.roomId, path) }

                val file = File(path)
                file.parentFile?.mkdirs()
                val eventFile = File("$path.event")

                val bufferedFos = BufferedOutputStream(FileOutputStream(file), 64 * 1024)
                val bufferedEventFos = BufferedOutputStream(FileOutputStream(eventFile), 8 * 1024)

                files[msg.roomId] = ActiveFile(
                    file = file, eventFile = eventFile,
                    fos = bufferedFos,
                    eventFos = bufferedEventFos,
                    roomId = msg.roomId,
                    roomName = msg.roomName,
                    startTime = msg.startTime,
                    quality = msg.quality
                )
            }
            logger.info("Opened file: $path")
        } catch (e: Exception) {
            logger.error("Failed to open file for room ${msg.roomId}: ${e.message}", e)
            eventBus.publish(WriterFatal(msg.roomId, e.message ?: "Unknown error"))
            files.remove(msg.roomId)?.dispose()
        }
    }

    private suspend fun handleStreamData(msg: StreamData) {
        val active = files[msg.roomId] ?: return
        try {
            withContext(Dispatchers.IO) {
                var data = msg.data
                logger.trace("Receive {} {}", msg.roomId, msg.meta.url)
                hooks.forEach { data = it.beforeWrite(msg.roomId, data) }
                active.fos.write(data)
                active.bytesWritten += data.size
            }
        } catch (e: Exception) {
            logger.error("Failed to write data for room ${msg.roomId}: ${e.message}", e)
            eventBus.publish(WriterFatal(msg.roomId, e.message ?: "Unknown error"))
            files.remove(msg.roomId)?.dispose()
        }
    }

    private suspend fun handleStreamEnd(msg: StreamEnd) {
        val active = files.remove(msg.roomId) ?: return
        parentScope.launch(Dispatchers.IO + NonCancellable) {
            closeActiveFile(active, msg.reason)
        }
    }

    private suspend fun handleStreamEvent(msg: StreamEvent) {
        val active = files[msg.roomId] ?: return
        try {
            withContext(Dispatchers.IO) {
                active.eventFos.write((msg.eventJson + "\n").toByteArray())
            }
        } catch (e: Exception) {
            logger.error("Failed to write event for room ${msg.roomId}: ${e.message}", e)
            eventBus.publish(WriterFatal(msg.roomId, e.message ?: "Unknown error"))
            files.remove(msg.roomId)?.dispose()
        }
    }

    private suspend fun closeActiveFile(active: ActiveFile, reason: EndReason) {
        withContext(NonCancellable) {
            try {
                // Ensure all buffered bytes in memory are written before closing
                runCatching { active.fos.flush() }
                runCatching { active.eventFos.flush() }

                if (active.bytesWritten < 1024) {
                    logger.info("Closed file: ${active.file.absolutePath}, reason=$reason (empty)")
                    active.dispose()
                    active.file.delete()
                    active.eventFile.delete()
                    return@withContext
                }
                active.fos.close()
                active.eventFos.close()

                val endTime = Instant.now()
                val durationMs = java.time.Duration.between(active.startTime, endTime).toMillis()
                val durFmt = formatDurationHM(durationMs)
                val finalName = "${active.roomName}-${timeFormatter.format(active.startTime)}-${durFmt}.mp4"
                val finalFile = File(tmpDir, finalName)
                val finalEvent = File(tmpDir, "$finalName.event")

                // Robust atomic file move across filesystems / mounts
                runCatching {
                    Files.move(active.file.toPath(), finalFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
                    Files.move(active.eventFile.toPath(), finalEvent.toPath(), StandardCopyOption.REPLACE_EXISTING)
                }.onFailure { e ->
                    logger.warn("Files.move failed, falling back to renameTo: ${e.message}")
                    active.file.renameTo(finalFile)
                    active.eventFile.renameTo(finalEvent)
                }

                if (finalEvent.exists() && finalEvent.length() == 0L) {
                    finalEvent.delete()
                }
                if (finalFile.length() == 0L) {
                    finalFile.delete()
                    logger.info("Remove empty file: ${finalFile.absolutePath}, reason=$reason")
                    return@withContext
                }

                hooks.forEach { it.afterFileClosed(active.roomId, finalFile) }
                eventBus.publish(
                    FileReady(
                        active.roomId,
                        finalFile,
                        reason,
                        active.roomName,
                        active.startTime.toEpochMilli(),
                        endTime.toEpochMilli(),
                        durationMs,
                        active.quality
                    )
                )
                logger.info("Closed file: ${finalFile.absolutePath}, reason=$reason")
            } catch (e: Exception) {
                logger.error("Failed to close file for room ${active.roomId}: ${e.message}", e)
                eventBus.publish(WriterFatal(active.roomId, e.message ?: "Unknown error"))
                active.dispose()
            }
        }
    }

    private fun formatDurationHM(ms: Long): String {
        val h = ms / 3600_000
        val m = (ms % 3600_000) / 60_000
        val s = (ms % 60_000) / 1000
        return if (h > 0) "${h}h${m}m${s}s" else if (m > 0) "${m}m${s}s" else "${s}s"
    }
}
