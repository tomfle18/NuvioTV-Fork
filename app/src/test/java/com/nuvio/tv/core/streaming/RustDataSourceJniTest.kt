package com.nuvio.tv.core.streaming

import android.net.Uri
import androidx.media3.common.ByteBufferDataReader
import androidx.media3.datasource.DataSource
import androidx.media3.datasource.DataSpec
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.RecordedRequest
import okhttp3.mockwebserver.Dispatcher
import okio.Buffer
import io.mockk.*
import org.junit.*
import org.junit.Assert.*
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.TimeUnit

/** Runs the production Kotlin adapter against the real host JNI, without claiming Android execution. */
@androidx.media3.common.util.UnstableApi
class RustDataSourceJniTest {
    private lateinit var server: MockWebServer
    private val live = ConcurrentHashMap.newKeySet<Long>()
    private val next = AtomicLong(1)
    private val duplicateRelease = AtomicInteger()
    private val headersDelay = AtomicLong()
    private val total = 5L * 1024 * 1024 * 1024 + 177
    private lateinit var url: String

    @Before fun start() {
        Assume.assumeTrue(System.getProperty("nuvio.rust.jni") == "true")
        assertNull(RustStreamingBridge.availability)
        mockkStatic(Uri::class)
        every { Uri.parse(any()) } answers {
            val text = firstArg<String>()
            mockk<Uri>(relaxed = true).also { every { it.toString() } returns text }
        }
        mockkObject(RustNetworkHooks)
        every { RustNetworkHooks.allowed(any()) } returns true
        every { RustNetworkHooks.capacity(any(), any()) } answers { secondArg() }
        every { RustNetworkHooks.history(any()) } returns LongArray(0)
        every { RustNetworkHooks.resolve(any()) } returns arrayOf("127.0.0.1")
        every { RustNetworkHooks.limit(any(), any(), any()) } just Runs
        every { RustNetworkHooks.acquire(any(), any()) } answers { next.getAndIncrement().also { live += it } }
        every { RustNetworkHooks.release(any(), any()) } answers {
            if (!live.remove(firstArg<Long>())) duplicateRelease.incrementAndGet()
            Unit
        }
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val range = request.getHeader("Range")!!.substring(6).split('-')
                val start = range[0].toLong()
                val end = minOf(range[1].toLong(), total - 1)
                return MockResponse().setResponseCode(206).setHeader("ETag", "\"adapter-v1\"")
                    .setHeader("Content-Range", "bytes $start-$end/$total")
                    .setBody(Buffer().write(ByteArray((end - start + 1).toInt()) { ((start + it) % 251).toByte() }))
                    .setHeadersDelay(headersDelay.getAndSet(0), TimeUnit.MILLISECONDS)
            }
        }
        server.start()
        url = server.url("/file.mkv").toString()
    }

    @After fun stop() {
        if (::server.isInitialized) server.shutdown()
        if (::url.isInitialized) { unmockkObject(RustNetworkHooks); unmockkStatic(Uri::class) }
    }

    @Test fun adapterReopenArrayHeapAndDirectBuffersPreserveBytesAndPositions() {
        val factory = RustHttpDataSource.Factory(url, emptyMap(), 2L * 1024 * 1024, 3, 131072, false,
            DataSource.Factory { error("unexpected Kotlin fallback") })
        try {
            val source = factory.createDataSource()
            repeat(60) { cycle ->
                val position = if (cycle % 2 == 0) cycle * 371L else (1L shl 32) + cycle * 7919L
                val spec = DataSpec.Builder().setUri(Uri.parse(url)).setPosition(position).setLength(12345).build()
                assertEquals(12345L, source.open(spec))
                assertEquals(url, source.uri.toString())
                var at = 0
                while (at < 12345) {
                    val buffer = if (cycle % 3 == 0) ByteBuffer.allocate(1050) else ByteBuffer.allocateDirect(1050)
                    buffer.position(13); buffer.limit(1013)
                    val n = (source as ByteBufferDataReader).read(buffer, minOf(1000, 12345 - at))
                    assertTrue(n > 0)
                    assertEquals(13 + n, buffer.position())
                    repeat(n) { assertEquals(((position + at + it) % 251).toInt(), buffer.get(it + 13).toInt() and 255) }
                    at += n
                }
                assertEquals(-1, source.read(ByteArray(1), 0, 1))
                assertEquals(0, source.read(ByteArray(0), 0, 0))
                source.close(); source.close()
            }
            assertTrue(factory.snapshot()!![2] <= 2L * 1024 * 1024)
        } finally { factory.close() }
        assertTrue("native request slot leaked", live.isEmpty())
        assertEquals(0, duplicateRelease.get())
    }

    @Test fun cancelledOldOpenCannotCloseTheReaderOfANewOpen() {
        val factory = RustHttpDataSource.Factory(url, emptyMap(), 2L * 1024 * 1024, 1, 131072, false,
            DataSource.Factory { error("unexpected Kotlin fallback") })
        val source = factory.createDataSource()
        val failed = AtomicReference<Throwable>()
        headersDelay.set(800)
        val opener = Thread {
            try { source.open(DataSpec.Builder().setUri(Uri.parse(url)).build()); failed.set(AssertionError("old open survived cancellation")) }
            catch (expected: java.io.IOException) { }
            catch (error: Throwable) { failed.set(error) }
        }
        try {
            opener.start()
            assertNotNull(server.takeRequest(1, TimeUnit.SECONDS))
            source.close()
            val position = (1L shl 32) + 77
            assertEquals(16L, source.open(DataSpec.Builder().setUri(Uri.parse(url)).setPosition(position).setLength(16).build()))
            opener.join(500)
            assertFalse(opener.isAlive)
            assertNull(failed.get())
            val bytes = ByteArray(16)
            assertEquals(16, source.read(bytes, 0, bytes.size))
            repeat(16) { assertEquals(((position + it) % 251).toInt(), bytes[it].toInt() and 255) }
        } finally { source.close(); factory.close(); opener.join(1000) }
        assertTrue(live.isEmpty())
        assertArrayEquals(longArrayOf(0, 0, 0), RustStreamingBridge.resources())
    }
}
