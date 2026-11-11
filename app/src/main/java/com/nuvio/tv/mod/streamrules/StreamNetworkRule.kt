package com.nuvio.tv.mod.streamrules

import com.nuvio.tv.data.local.PlayerSettings
import org.json.JSONArray
import org.json.JSONObject

/**
 * Fork-only: the three Network settings (Automatic speed-up, Connections, Chunk size) for streams whose address,
 * as the addon lists it and before any redirect, starts with [urlPrefix]. Everything else stays global.
 */
data class StreamNetworkRule(
    val id: String,
    val urlPrefix: String,
    val autoSpeedup: Boolean,
    /** [CONNECTIONS_AUTOMATIC], 1, or the number of parallel connections. */
    val connections: Int,
    val chunkSizeKb: Int
) {
    fun matches(url: String): Boolean {
        val prefix = urlPrefix.trim()
        if (prefix.isEmpty()) return false
        // A prefix typed without a scheme ("host.example/path") matches both http and https.
        val candidate = if ("://" in prefix) url.trim() else url.trim().substringAfter("://")
        return candidate.startsWith(prefix, ignoreCase = true)
    }

    /** [settings] with this rule's Network values; the same writes the Connections slider makes globally. */
    fun applyTo(settings: PlayerSettings): PlayerSettings {
        val parallel = connections >= 2
        return settings.copy(
            autoSpeedupEnabled = autoSpeedup,
            parallelNetworkEnabled = connections != CONNECTIONS_AUTOMATIC,
            useParallelConnections = parallel,
            parallelConnectionCount = if (parallel) {
                connections.coerceAtMost(PlayerSettings.maxParallelConnectionCount(settings.nuvioPerformanceModeEnabled))
            } else {
                settings.parallelConnectionCount
            },
            parallelChunkSizeKb = if (parallel) chunkSizeKb else settings.parallelChunkSizeKb
        )
    }

    companion object {
        const val CONNECTIONS_AUTOMATIC = 0
    }
}

object StreamNetworkRules {
    /** The first rule in list order that matches [url]; order is the priority. */
    fun firstMatch(rules: List<StreamNetworkRule>, url: String?): StreamNetworkRule? =
        if (url.isNullOrBlank()) null else rules.firstOrNull { it.matches(url) }

    fun apply(rules: List<StreamNetworkRule>, url: String?, settings: PlayerSettings): PlayerSettings =
        firstMatch(rules, url)?.applyTo(settings) ?: settings

    fun toJson(rules: List<StreamNetworkRule>): String = JSONArray().apply {
        rules.forEach { rule ->
            put(
                JSONObject()
                    .put("id", rule.id)
                    .put("urlPrefix", rule.urlPrefix)
                    .put("autoSpeedup", rule.autoSpeedup)
                    .put("connections", rule.connections)
                    .put("chunkSizeKb", rule.chunkSizeKb)
            )
        }
    }.toString()

    fun fromJson(json: String?): List<StreamNetworkRule> {
        if (json.isNullOrBlank()) return emptyList()
        return runCatching {
            val array = JSONArray(json)
            (0 until array.length()).mapNotNull { index ->
                val item = array.optJSONObject(index) ?: return@mapNotNull null
                val prefix = item.optString("urlPrefix").trim()
                if (prefix.isEmpty()) return@mapNotNull null
                StreamNetworkRule(
                    id = item.optString("id").ifBlank { "rule-$index" },
                    urlPrefix = prefix,
                    autoSpeedup = item.optBoolean("autoSpeedup", true),
                    connections = item.optInt("connections", StreamNetworkRule.CONNECTIONS_AUTOMATIC).coerceAtLeast(0),
                    chunkSizeKb = item.optInt("chunkSizeKb", PlayerSettings.DEFAULT_PARALLEL_CHUNK_SIZE_KB)
                )
            }
        }.getOrDefault(emptyList())
    }
}
