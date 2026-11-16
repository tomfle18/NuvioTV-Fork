package com.nuvio.tv.core.streaming

import androidx.annotation.Keep
import java.nio.ByteBuffer

/** Fixed, versioned JNI surface. No pointer or native block is exposed to the JVM. */
@Keep
internal object RustStreamingBridge {
    val availability: String? by lazy {
        try {
            System.loadLibrary("nuvio_streaming")
            if (version() == 1) null else "contract_version"
        } catch (_: LinkageError) {
            "native_library_unavailable"
        } catch (_: SecurityException) {
            "native_library_unavailable"
        }
    }

    external fun version(): Int
    external fun create(url: String, headers: Array<String>, budget: Long, connections: Int, piece: Long,
        prefetch: Int, adaptive: Int, hooks: RustNetworkHooks): Long
    external fun open(session: Long, position: Long, length: Long): Long
    external fun awaitOpen(reader: Long): Long
    external fun readArray(reader: Long, bytes: ByteArray, offset: Int, length: Int): Int
    external fun readDirect(reader: Long, buffer: ByteBuffer, position: Int, length: Int): Int
    external fun cancel(reader: Long)
    external fun closeReader(reader: Long)
    external fun releaseSession(session: Long)
    external fun updateLimit(session: Long, bytes: Long)
    /** network, delivered, allocated (including pool), free pool, limit, live requests, request count */
    external fun stats(session: Long): LongArray
    /** Final URI followed by alternating header names and values, retaining duplicate header values. */
    external fun info(session: Long): Array<String>
}
