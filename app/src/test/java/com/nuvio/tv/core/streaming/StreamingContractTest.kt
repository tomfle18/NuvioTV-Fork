package com.nuvio.tv.core.streaming

import org.junit.Assert.*
import org.junit.Test
import java.io.IOException

class StreamingContractTest {
    @Test fun restartCircuitSurvivesRebuildAndDistinguishesAuthorizationContexts() {
        val gate = StreamingFallbackGate()
        val error = IOException("rust-streaming:Network")
        assertTrue(gate.take("https://fixture/file", mapOf("Authorization" to "one"), error))
        assertFalse(gate.take("https://fixture/file", mapOf("Authorization" to "one"), error))
        assertTrue(gate.take("https://fixture/file", mapOf("Authorization" to "two"), error))
        assertFalse(gate.take("https://fixture/other", emptyMap(), IOException("rust-streaming:Changed")))
        gate.sourceChanged("https://fixture/other")
        assertFalse(gate.blocked("https://fixture/file", mapOf("Authorization" to "two")))
    }
    @Test fun fallbackOnlyBeforeDeliveryForRecoverableTransportFailures() {
        listOf("Unsupported", "Network", "Timeout", "Budget").forEach {
            assertTrue(RustFailurePolicy.canFallbackBeforeRead(IOException("rust-streaming:$it")))
        }
        listOf("Changed", "Protocol", "Invalid", "Cancelled", "Internal", "Http(401)", "Http(403)").forEach {
            assertFalse(RustFailurePolicy.canFallbackBeforeRead(IOException("rust-streaming:$it")))
        }
    }
    @Test fun networkBudgetReservesSamplesAndRemainsBoundedOnLargeDevices() {
        val mb = 1024L * 1024
        assertEquals(0L, StreamingBudgetBridge.networkBytes(16 * mb))
        assertEquals(4 * mb, StreamingBudgetBridge.networkBytes(64 * mb))
        assertEquals(16 * mb, StreamingBudgetBridge.networkBytes(1024 * mb))
        assertTrue(StreamingBudgetBridge.networkBytes(64 * mb) + 32 * mb <= 64 * mb)
        assertEquals(0L, StreamingBudgetBridge.allowance(16 * mb, 50 * mb, 50 * mb, 0))
        val allowed = StreamingBudgetBridge.allowance(16 * mb, 100 * mb, 50 * mb, 95_000_000)
        assertEquals(100 * mb - 95_000_000 - 65536, allowed)
        assertEquals(0L, StreamingBudgetBridge.allowance(16 * mb, 100 * mb, 50 * mb, Long.MAX_VALUE))
    }
}
