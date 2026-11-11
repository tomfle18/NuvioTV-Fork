package com.nuvio.tv.mod.streamrules

import android.content.Context
import android.util.Log
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.emptyPreferences
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import com.nuvio.tv.data.local.PlayerSettings
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map

private val Context.streamNetworkRulesDataStore by preferencesDataStore(
    name = "mod_stream_network_rules",
    corruptionHandler = androidx.datastore.core.handlers.ReplaceFileCorruptionHandler { emptyPreferences() }
)

/** Fork-only: the saved rules, in their own file so the upstream settings store stays untouched. */
object StreamNetworkRulesStore {
    private const val TAG = "StreamNetworkRules"
    private val rulesKey = stringPreferencesKey("rules_json")

    /** Last list read or saved, for the callers that cannot wait on the store. */
    @Volatile private var cachedRules: List<StreamNetworkRule> = emptyList()

    /** The global settings of the last playback start, before any rule was applied. */
    @Volatile private var lastGlobalSettings: PlayerSettings? = null

    fun flow(context: Context): Flow<List<StreamNetworkRule>> =
        context.applicationContext.streamNetworkRulesDataStore.data
            .map { prefs -> StreamNetworkRules.fromJson(prefs[rulesKey]).also { cachedRules = it } }

    suspend fun save(context: Context, rules: List<StreamNetworkRule>) {
        cachedRules = rules
        context.applicationContext.streamNetworkRulesDataStore.edit { it[rulesKey] = StreamNetworkRules.toJson(rules) }
    }

    /** Playback start: [global] with the first rule matching [url], read fresh from the store. */
    suspend fun resolve(context: Context, url: String, global: PlayerSettings): PlayerSettings {
        lastGlobalSettings = global
        val rules = runCatching { flow(context).first() }.getOrDefault(cachedRules)
        return applyAndLog(rules, url, global)
    }

    /**
     * The chunk-0 pre-start of an episode switch: it only has the previous stream's snapshot, which may already carry
     * that stream's rule, so the rule for [url] goes on top of the last global settings instead.
     */
    fun resolveCached(url: String, previous: PlayerSettings): PlayerSettings =
        applyAndLog(cachedRules, url, lastGlobalSettings ?: previous)

    private fun applyAndLog(rules: List<StreamNetworkRule>, url: String, global: PlayerSettings): PlayerSettings {
        val rule = StreamNetworkRules.firstMatch(rules, url) ?: return global
        Log.i(
            TAG,
            "rule '${rule.urlPrefix}' applied: autoSpeedup=${rule.autoSpeedup} " +
                "connections=${rule.connections} chunkKb=${rule.chunkSizeKb}"
        )
        return rule.applyTo(global)
    }
}
