package com.nuvio.tv.mod.streamrules

import com.nuvio.tv.data.local.PlayerSettings
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class StreamNetworkRuleTest {
    private fun rule(prefix: String, connections: Int = 4, autoSpeedup: Boolean = false, chunkKb: Int = 8192) =
        StreamNetworkRule(id = prefix, urlPrefix = prefix, autoSpeedup = autoSpeedup, connections = connections, chunkSizeKb = chunkKb)

    @Test
    fun `matches the start of the address ignoring case`() {
        val rule = rule("https://Host.example/play/")
        assertTrue(rule.matches("https://host.example/play/abc?token=1"))
        assertFalse(rule.matches("https://cdn.other.example/redirected/host.example/play/"))
        assertFalse(rule.matches("https://host.example/other"))
    }

    @Test
    fun `prefix without a scheme matches http and https`() {
        val rule = rule("host.example/play")
        assertTrue(rule.matches("http://host.example/play/1"))
        assertTrue(rule.matches("https://HOST.example/play/1"))
        assertFalse(rule.matches("https://sub.host.example/play/1"))
    }

    @Test
    fun `blank prefix never matches`() {
        assertFalse(rule("  ").matches("https://host.example/"))
    }

    @Test
    fun `first matching rule in list order wins`() {
        val broad = rule("https://host.example/", connections = 2)
        val narrow = rule("https://host.example/play/", connections = 6)
        assertSame(broad, StreamNetworkRules.firstMatch(listOf(broad, narrow), "https://host.example/play/1"))
        assertSame(narrow, StreamNetworkRules.firstMatch(listOf(narrow, broad), "https://host.example/play/1"))
        assertNull(StreamNetworkRules.firstMatch(listOf(broad, narrow), "https://elsewhere.example/"))
        assertNull(StreamNetworkRules.firstMatch(listOf(broad), null))
    }

    @Test
    fun `no match leaves the settings untouched`() {
        val settings = PlayerSettings()
        assertSame(settings, StreamNetworkRules.apply(listOf(rule("https://host.example/")), "https://x.example/", settings))
    }

    @Test
    fun `parallel rule sets only the network values`() {
        val global = PlayerSettings(autoSpeedupEnabled = true, parallelNetworkEnabled = false)
        val applied = rule("h", connections = 4, autoSpeedup = false, chunkKb = 4096).applyTo(global)
        assertEquals(
            global.copy(
                autoSpeedupEnabled = false,
                parallelNetworkEnabled = true,
                useParallelConnections = true,
                parallelConnectionCount = 4,
                parallelChunkSizeKb = 4096
            ),
            applied
        )
    }

    @Test
    fun `connection count is capped at what the box allows`() {
        val global = PlayerSettings()
        val applied = rule("h", connections = 999).applyTo(global)
        assertEquals(PlayerSettings.maxParallelConnectionCount(global.nuvioPerformanceModeEnabled), applied.parallelConnectionCount)
    }

    @Test
    fun `single connection rule turns parallel off and keeps the global chunk size`() {
        val global = PlayerSettings(parallelNetworkEnabled = true, useParallelConnections = true, parallelChunkSizeKb = 16384)
        val applied = rule("h", connections = 1, chunkKb = 256).applyTo(global)
        assertTrue(applied.parallelNetworkEnabled)
        assertFalse(applied.useParallelConnections)
        assertEquals(16384, applied.parallelChunkSizeKb)
    }

    @Test
    fun `automatic rule overrides a global custom connection count`() {
        val global = PlayerSettings(parallelNetworkEnabled = true, useParallelConnections = true, autoSpeedupEnabled = false)
        val applied = rule("h", connections = StreamNetworkRule.CONNECTIONS_AUTOMATIC, autoSpeedup = true).applyTo(global)
        assertFalse(applied.parallelNetworkEnabled)
        assertFalse(applied.useParallelConnections)
        assertTrue(applied.autoSpeedupEnabled)
    }

    @Test
    fun `json round trip keeps order and values`() {
        val rules = listOf(rule("https://a.example/", 2, true, 1024), rule("b.example", 0, false, 8192))
        assertEquals(rules, StreamNetworkRules.fromJson(StreamNetworkRules.toJson(rules)))
    }

    @Test
    fun `broken or empty json reads as no rules`() {
        assertEquals(emptyList<StreamNetworkRule>(), StreamNetworkRules.fromJson(null))
        assertEquals(emptyList<StreamNetworkRule>(), StreamNetworkRules.fromJson("not json"))
        assertEquals(emptyList<StreamNetworkRule>(), StreamNetworkRules.fromJson("""[{"urlPrefix":"  "}]"""))
    }
}
