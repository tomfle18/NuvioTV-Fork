package com.nuvio.tv.core.streaming

import android.net.Uri
import androidx.annotation.OptIn
import androidx.media3.common.ByteBufferDataReader
import androidx.media3.common.util.UnstableApi
import androidx.media3.datasource.BaseDataSource
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSpec
import androidx.media3.datasource.HttpDataSource
import com.nuvio.tv.core.player.MemoryPressure
import kotlinx.coroutines.flow.MutableStateFlow
import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

internal object StreamingDiagnostics {
    val status = MutableStateFlow("Kotlin")
    fun kotlin(reason: String) { status.value = "Kotlin ($reason)" }
    fun rust() { status.value = "Rust (experimental)" }
    @Volatile var metrics: LongArray = LongArray(10)
    fun metricsLabel(): String {
        val m = metrics
        return "backend=${status.value} rustNetwork=${m[0]} rustDelivered=${m[1]} rustAllocated=${m[2]} " +
            "rustPool=${m[3]} rustLimit=${m[4]} rustLive=${m[5]} rustRequests=${m[6]} rustDownloads=${m[7]} rustPiece=${m[8]} rustPeak=${m[9]}"
    }
}

/** A spent native retry budget must not be multiplied by Media3's loader retries. */
internal class RustTerminalIOException(cause: IOException, val sourceToken: Long = 0) : IOException("Rust transport stopped", cause)

internal object RustFailurePolicy {
    fun canFallbackBeforeRead(error: IOException): Boolean = error.message in setOf(
        "rust-streaming:Unsupported", "rust-streaming:Network", "rust-streaming:Timeout", "rust-streaming:Budget"
    )
}

@OptIn(UnstableApi::class)
internal class RustHttpDataSource private constructor(private val owner: Factory) : BaseDataSource(true), ByteBufferDataReader {
    private class Open(val spec: DataSpec) {
        val reader = AtomicLong(0)
        @Volatile var cancelled = false
        @Volatile var fallback: DataSource? = null
        var started = false
        var uri: Uri? = null
        var headers: Map<String, List<String>> = emptyMap()
    }
    private val lifecycle = Any()
    @Volatile private var active: Open? = null
    private val scratch by lazy { ByteArray(16 * 1024) }

    private fun current(state: Open) = active === state && !state.cancelled
    private fun cancelled() = IOException("rust-streaming:Cancelled")

    override fun open(dataSpec: DataSpec): Long {
        val state = Open(dataSpec)
        synchronized(lifecycle) {
            check(active == null) { "DataSource already open" }
            owner.attach(this)
            active = state
            transferInitializing(dataSpec)
        }
        try {
            if (!owner.accepts(dataSpec)) {
                if (!current(state)) throw cancelled()
                owner.disable("unsupported_request")
                return openFallback(state, "unsupported_request")
            }
            owner.disabledReason?.let { return openFallback(state, it) }
            val session = owner.session()
            val id = RustStreamingBridge.open(session, dataSpec.position, dataSpec.length)
            state.reader.set(id)
            if (!current(state)) throw cancelled()
            val length = RustStreamingBridge.awaitOpen(id)
            val info = if (dataSpec.length != 0L) RustStreamingBridge.info(session) else null
            synchronized(lifecycle) {
                if (!current(state)) throw cancelled()
                state.uri = info?.let { Uri.parse(it[0]) } ?: dataSpec.uri
                state.headers = info?.drop(1)?.chunked(2)?.groupBy({ it[0] }, { it[1] }) ?: emptyMap()
                state.started = true
                transferStarted(dataSpec)
                StreamingDiagnostics.rust()
            }
            return length
        } catch (error: IOException) {
            closeNativeReader(state)
            if (!current(state)) throw cancelled()
            if (state.fallback != null) throw error
            if (RustFailurePolicy.canFallbackBeforeRead(error)) {
                val reason = "native_" + error.message.orEmpty().substringAfter(':').lowercase()
                owner.disable(reason)
                return openFallback(state, reason)
            }
            throw translate(state, error)
        }
    }

    private fun openFallback(state: Open, reason: String): Long {
        if (!current(state)) throw cancelled()
        val source = owner.fallbackFactory.createDataSource()
        state.fallback = source
        try {
            if (!current(state)) throw cancelled()
            val length = source.open(state.spec)
            synchronized(lifecycle) {
                if (!current(state)) throw cancelled()
                state.uri = source.uri
                state.headers = source.responseHeaders
                state.started = true
                transferStarted(state.spec)
                StreamingDiagnostics.kotlin(reason)
            }
            return length
        } catch (error: IOException) {
            source.close()
            throw error
        }
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        require(offset >= 0 && length >= 0 && offset <= buffer.size && length <= buffer.size - offset)
        if (length == 0) return 0
        val state = active ?: throw cancelled()
        return deliver(state) {
            state.fallback?.read(buffer, offset, length)
                ?: RustStreamingBridge.readArray(state.reader.get(), buffer, offset, length)
        }
    }

    override fun supportsByteBufferRead() = true
    override fun read(buffer: ByteBuffer, length: Int): Int {
        require(length >= 0 && length <= buffer.remaining() && !buffer.isReadOnly)
        if (length == 0) return 0
        val state = active ?: throw cancelled()
        val source = state.fallback
        if (source is ByteBufferDataReader && source.supportsByteBufferRead()) {
            return deliver(state) { source.read(buffer, length) }
        }
        if (source != null || !buffer.isDirect) {
            val n = deliver(state) {
                source?.read(scratch, 0, minOf(length, scratch.size))
                    ?: RustStreamingBridge.readArray(state.reader.get(), scratch, 0, minOf(length, scratch.size))
            }
            if (n > 0) buffer.put(scratch, 0, n)
            return n
        }
        val position = buffer.position()
        val n = deliver(state) { RustStreamingBridge.readDirect(state.reader.get(), buffer, position, length) }
        if (n > 0) buffer.position(position + n)
        return n
    }

    private inline fun deliver(state: Open, read: () -> Int): Int = try {
        val n = read()
        synchronized(lifecycle) {
            if (!current(state)) throw cancelled()
            if (n > 0) bytesTransferred(n)
        }
        n
    } catch (error: IOException) {
        if (!current(state)) throw cancelled()
        throw if (state.fallback != null) error else translate(state, error)
    }

    private fun translate(state: Open, error: IOException): IOException {
        val code = Regex("^rust-streaming:Http\\((\\d+)\\)$").matchEntire(error.message.orEmpty())?.groupValues?.get(1)?.toIntOrNull()
        if (code != null) {
            val http = HttpDataSource.InvalidResponseCodeException(code, "Rust HTTP", error, state.headers, state.spec, ByteArray(0))
            return if (code in setOf(401, 403, 404, 410, 416)) http else RustTerminalIOException(http, owner.token)
        }
        if (error.message == "rust-streaming:Cancelled") return error
        val terminal = RustTerminalIOException(error, owner.token)
        val networkCode = when (error.message) {
            "rust-streaming:Network" -> androidx.media3.common.PlaybackException.ERROR_CODE_IO_NETWORK_CONNECTION_FAILED
            "rust-streaming:Timeout" -> androidx.media3.common.PlaybackException.ERROR_CODE_IO_NETWORK_CONNECTION_TIMEOUT
            else -> return terminal
        }
        return HttpDataSource.HttpDataSourceException("Rust transport stopped", terminal, state.spec, networkCode,
            if (state.started) HttpDataSource.HttpDataSourceException.TYPE_READ else HttpDataSource.HttpDataSourceException.TYPE_OPEN)
    }

    override fun getUri(): Uri? = active?.let { it.fallback?.uri ?: it.uri }
    override fun getResponseHeaders(): Map<String, List<String>> = active?.let { it.fallback?.responseHeaders ?: it.headers } ?: emptyMap()

    private fun closeNativeReader(state: Open) {
        val id = state.reader.getAndSet(0)
        if (id != 0L) { RustStreamingBridge.cancel(id); RustStreamingBridge.closeReader(id) }
    }
    override fun close() {
        val state = synchronized(lifecycle) {
            val state = active ?: return
            active = null
            state.cancelled = true
            if (state.started) { state.started = false; transferEnded() }
            state
        }
        try {
            closeNativeReader(state)
            state.fallback?.close()
        } finally {
            synchronized(lifecycle) { if (active == null) owner.detach(this) }
        }
    }

    /** One film/source owns this object. It survives DataSource close/open and dies on shutdown/source change. */
    class Factory(
        val url: String,
        headers: Map<String, String>,
        val budget: Long,
        private val connections: Int,
        private val piece: Long,
        private val adaptive: Boolean,
        @Volatile var fallbackFactory: DataSource.Factory,
        private val onDisabled: () -> Unit = {}
    ) : DataSource.Factory, AutoCloseable {
        val headers = headers.toMap()
        val token = tokens.getAndIncrement()
        private var handle = 0L
        @Volatile private var closed = false
        private var warmStarted = false
        private var sampledAt = -1L
        private var targetBudget = budget
        @Volatile var reservedBytes: Long = budget
            private set
        @Volatile var connectionReservation: Int = if (adaptive) 1 else connections
            private set
        private val warmReader = AtomicLong(0)
        @Volatile var disabledReason: String? = null
            private set
        private val sources = ConcurrentHashMap.newKeySet<RustHttpDataSource>()
        private val pressure: (Int) -> Unit = {
            synchronized(this) {
                targetBudget = minOf(targetBudget, 512L * 1024)
                if (handle != 0L) RustStreamingBridge.updateLimit(handle, targetBudget)
            }
        }
        init { MemoryPressure.addListener(android.content.ComponentCallbacks2.TRIM_MEMORY_RUNNING_LOW, pressure) }
        @Synchronized fun attach(source: RustHttpDataSource) {
            if (closed) throw IOException("rust-streaming:Cancelled")
            sources += source
        }
        fun detach(source: RustHttpDataSource) { sources -= source }
        @Synchronized fun prewarm() {
            if (warmStarted || closed || disabledReason != null) return
            warmStarted = true
            Thread({
                try {
                    val id = synchronized(this) {
                        if (closed) return@Thread
                        RustStreamingBridge.open(session(), 0, 64L * 1024).also { warmReader.set(it) }
                    }
                    RustStreamingBridge.awaitOpen(id)
                    val scratch = ByteArray(16 * 1024)
                    var remaining = 64 * 1024
                    while (remaining > 0) {
                        val n = RustStreamingBridge.readArray(id, scratch, 0, minOf(scratch.size, remaining))
                        if (n < 0) break
                        remaining -= n
                    }
                } catch (_: IOException) {
                    // Speculative failure is reported by the real open; it never starts another backend.
                } finally {
                    val id = warmReader.getAndSet(0)
                    if (id != 0L) RustStreamingBridge.closeReader(id)
                }
            }, "rust-prewarm").apply { isDaemon = true; start() }
        }
        fun matches(url: String, headers: Map<String, String>) = this.url == url && this.headers == headers && !closed
        fun accepts(spec: DataSpec): Boolean = spec.uri.toString() == url && spec.httpMethod == DataSpec.HTTP_METHOD_GET &&
            spec.httpBody == null && headers.keys.none { it.lowercase() in setOf("range", "if-range", "host", "content-length") } &&
            spec.httpRequestHeaders.all { (k, v) -> headers.entries.any { it.key.equals(k, true) && it.value == v } }
        @Synchronized fun session(): Long {
            if (closed) throw IOException("rust-streaming:Cancelled")
            if (handle == 0L) {
                val effective = headers.toMutableMap()
                if (effective.keys.none { it.equals("User-Agent", true) }) effective["User-Agent"] =
                    com.nuvio.tv.ui.screens.player.PlayerMediaSourceFactory.DEFAULT_USER_AGENT
                handle = RustStreamingBridge.create(url, effective.flatMap { listOf(it.key, it.value) }.toTypedArray(),
                    budget, connections, piece, 1, if (adaptive) 1 else 0, RustNetworkHooks)
                if (targetBudget < budget) RustStreamingBridge.updateLimit(handle, targetBudget)
            }
            return handle
        }
        @Synchronized fun disable(reason: String) {
            if (closed) throw IOException("rust-streaming:Cancelled")
            disabledReason = reason
            if (handle != 0L) { RustStreamingBridge.releaseSession(handle); handle = 0 }
            onDisabled()
        }
        override fun createDataSource(): DataSource {
            check(!closed)
            return RustHttpDataSource(this)
        }
        fun snapshot(): LongArray? = synchronized(this) { if (handle == 0L) null else RustStreamingBridge.stats(handle) }
        @Synchronized fun adjustBudget(allowed: Long): Long {
            val next = minOf(targetBudget, allowed.coerceAtLeast(0))
            if (handle != 0L) {
                if (next < targetBudget) RustStreamingBridge.updateLimit(handle, next)
                // A lease can outlive eviction. Keep its memory reserved until the last reader releases it.
                reservedBytes = maxOf(next, RustStreamingBridge.stats(handle)[2])
            } else { reservedBytes = next }
            targetBudget = next
            return reservedBytes
        }
        @Synchronized fun sample(values: LongArray) {
            if (handle == 0L || disabledReason != null || values[0] - sampledAt < 1000) return
            sampledAt = values[0]
            try {
                RustStreamingBridge.sample(handle, values)
                StreamingDiagnostics.metrics = RustStreamingBridge.stats(handle)
                connectionReservation = StreamingDiagnostics.metrics[7].toInt().coerceAtLeast(1)
                com.nuvio.tv.core.player.thumbnail.ConnectionLedger.app.notePlaybackDownloads(url, connectionReservation)
            } catch (_: IOException) {
                // The loader reports terminal data errors. Metrics must never crash the UI thread.
            }
        }
        override fun close() {
            synchronized(this) { if (closed) return; closed = true }
            sources.forEach { try { it.close() } catch (_: IOException) { } }; sources.clear()
            val warm = warmReader.getAndSet(0)
            if (warm != 0L) { RustStreamingBridge.cancel(warm); RustStreamingBridge.closeReader(warm) }
            synchronized(this) {
                if (handle != 0L) {
                    try { RustStreamingBridge.releaseSession(handle) }
                    catch (_: IOException) { StreamingDiagnostics.kotlin("native_shutdown_deadline") }
                    finally { handle = 0 }
                }
            }
            MemoryPressure.removeListener(pressure)
        }
        companion object { private val tokens = AtomicLong(1) }
    }
}
