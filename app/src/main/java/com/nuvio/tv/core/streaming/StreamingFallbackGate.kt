package com.nuvio.tv.core.streaming

/** One restart per full source identity. Survives player release/rebuild, never logs credentials. */
internal class StreamingFallbackGate {
    private var failedUrl: String? = null
    private var failedHeaders: Map<String, String> = emptyMap()
    fun blocked(url: String, headers: Map<String, String>) = failedUrl == url && failedHeaders == headers
    fun take(url: String, headers: Map<String, String>, error: java.io.IOException): Boolean {
        if (blocked(url, headers) || !RustFailurePolicy.canFallbackBeforeRead(error)) return false
        failedUrl = url
        failedHeaders = headers.toMap()
        return true
    }
    fun sourceChanged(url: String) { if (url != failedUrl) { failedUrl = null; failedHeaders = emptyMap() } }
}
