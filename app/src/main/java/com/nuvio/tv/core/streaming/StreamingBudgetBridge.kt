package com.nuvio.tv.core.streaming

/** Bounded network allowance carved out of the existing playback buffer budget. */
internal object StreamingBudgetBridge {
    fun allowance(wanted: Long, total: Long, sampleFloor: Long, bitrate: Long): Long {
        val seconds = (bitrate.coerceAtLeast(0) / 8) * 8
        val startFloor = if (seconds == 0L) 0L else seconds.coerceAtMost(Long.MAX_VALUE - 65536) + 65536
        val floor = maxOf(sampleFloor, startFloor).coerceAtMost(total.coerceAtLeast(0))
        return minOf(wanted.coerceAtLeast(0), (total - floor).coerceAtLeast(0))
    }
    fun networkBytes(playbackBytes: Long): Long =
        ((playbackBytes - 32L * 1024 * 1024).coerceAtLeast(0) / 8).coerceAtMost(16L * 1024 * 1024)
}
