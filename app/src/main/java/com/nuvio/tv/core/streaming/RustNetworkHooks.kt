package com.nuvio.tv.core.streaming

import androidx.annotation.Keep
import com.nuvio.tv.NuvioApplication
import com.nuvio.tv.core.network.IPv4FirstDns
import com.nuvio.tv.core.network.ServerTrust
import com.nuvio.tv.core.player.thumbnail.ConnectionLedger
import com.nuvio.tv.core.player.thumbnail.Refusal
import com.nuvio.tv.ui.screens.player.HostSpeeds
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Request-level callbacks only. Never called for each read or body fragment. */
@Keep
internal object RustNetworkHooks {
    private data class Held(val url: String, val slot: ConnectionLedger.Slot)
    private val next = AtomicLong(1)
    private val slots = ConcurrentHashMap<Long, Held>()

    fun acquire(url: String, wanted: Int): Long {
        val hostCap = HostSpeeds.keyOf(url)?.let { HostSpeeds.limitOf(it) } ?: wanted
        val slot = ConnectionLedger.app.tryStreaming(url, minOf(wanted, hostCap)) ?: return 0
        val id = next.getAndIncrement()
        if (id <= 0) { slot.close(); return 0 }
        slots[id] = Held(url, slot)
        return id
    }

    fun release(token: Long, status: Int) {
        val held = slots.remove(token) ?: return
        try {
            when (status) {
                429 -> held.slot.refused(Refusal.TOO_MANY)
                503 -> held.slot.refused(Refusal.UNAVAILABLE)
                0 -> held.slot.refused(Refusal.CONNECTION)
                in 200..299 -> held.slot.succeeded()
            }
        } finally { held.slot.close() }
    }

    fun allowed(url: String): Boolean {
        val parsed = url.toHttpUrlOrNull() ?: return false
        // Configured media servers and the mutable extension cookie context retain Android's transport.
        return !ServerTrust.isServerHost(parsed.host) && parsed.username.isEmpty() && parsed.password.isEmpty() &&
            NuvioApplication.extensionCookieJar.loadForRequest(parsed).isEmpty()
    }

    fun resolve(host: String): Array<String> = IPv4FirstDns().lookup(host).mapNotNull { it.hostAddress }.toTypedArray()
}
