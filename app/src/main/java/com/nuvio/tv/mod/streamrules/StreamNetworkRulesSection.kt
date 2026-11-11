package com.nuvio.tv.mod.streamrules

import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import com.nuvio.tv.R
import com.nuvio.tv.data.local.PlayerSettings
import com.nuvio.tv.ui.components.NuvioDialog
import com.nuvio.tv.ui.screens.collection.NuvioTextField
import com.nuvio.tv.ui.screens.detail.requestFocusAfterFrames
import com.nuvio.tv.ui.screens.settings.MemoryBudget
import com.nuvio.tv.ui.screens.settings.SettingsActionRow
import com.nuvio.tv.ui.screens.settings.SettingsDialogActionButton
import com.nuvio.tv.ui.screens.settings.SettingsDialogActionRow
import com.nuvio.tv.ui.screens.settings.SettingsNote
import com.nuvio.tv.ui.screens.settings.SettingsSectionLabel
import com.nuvio.tv.ui.screens.settings.SettingsToggleRow
import com.nuvio.tv.ui.screens.settings.SliderSettingsItem
import kotlinx.coroutines.launch
import java.util.UUID

/** Fork-only: the list of per-address network rules under the Network settings, and their editor. */
@Composable
internal fun StreamNetworkRulesSection(settings: PlayerSettings) {
    val context = LocalContext.current.applicationContext
    val scope = rememberCoroutineScope()
    val rules by StreamNetworkRulesStore.flow(context).collectAsState(initial = emptyList())
    var editingId by remember { mutableStateOf<String?>(null) }
    var newRule by remember { mutableStateOf<StreamNetworkRule?>(null) }
    val save: (List<StreamNetworkRule>) -> Unit = { list -> scope.launch { StreamNetworkRulesStore.save(context, list) } }

    SettingsSectionLabel(text = stringResource(R.string.mod_stream_rules_label))
    SettingsNote(text = stringResource(R.string.mod_stream_rules_note))
    rules.forEachIndexed { index, rule ->
        SettingsActionRow(
            title = rule.urlPrefix,
            subtitle = ruleSummary(rule),
            value = "#${index + 1}",
            onClick = { editingId = rule.id },
            jumpAnchor = false
        )
    }
    SettingsActionRow(
        title = stringResource(R.string.mod_stream_rules_add),
        subtitle = null,
        onClick = {
            newRule = StreamNetworkRule(
                id = UUID.randomUUID().toString(),
                urlPrefix = "",
                autoSpeedup = settings.autoSpeedupEnabled,
                connections = StreamNetworkRule.CONNECTIONS_AUTOMATIC,
                chunkSizeKb = settings.parallelChunkSizeKb
            )
        },
        jumpAnchor = false
    )

    val index = rules.indexOfFirst { it.id == editingId }
    val draftSource = newRule ?: rules.getOrNull(index)
    if (draftSource != null) {
        val close = {
            editingId = null
            newRule = null
        }
        StreamNetworkRuleDialog(
            initial = draftSource,
            settings = settings,
            position = if (index >= 0) index + 1 else rules.size + 1,
            total = if (index >= 0) rules.size else rules.size + 1,
            existing = index >= 0,
            onSave = { draft ->
                save(if (index >= 0) rules.toMutableList().also { it[index] = draft } else rules + draft)
                close()
            },
            onMove = { draft, delta ->
                val target = index + delta
                if (index >= 0 && target in rules.indices) {
                    save(rules.toMutableList().also {
                        it.removeAt(index)
                        it.add(target, draft)
                    })
                }
            },
            onDelete = {
                save(rules.filterNot { it.id == draftSource.id })
                close()
            },
            onDismiss = close
        )
    }
}

@Composable
private fun ruleSummary(rule: StreamNetworkRule): String = stringResource(
    if (rule.autoSpeedup) R.string.mod_stream_rules_summary_speedup_on else R.string.mod_stream_rules_summary_speedup_off,
    connectionsLabel(rule.connections)
)

@Composable
private fun connectionsLabel(connections: Int): String =
    if (connections == StreamNetworkRule.CONNECTIONS_AUTOMATIC) {
        stringResource(R.string.playback_net_connections_auto)
    } else {
        connections.toString()
    }

@Composable
private fun StreamNetworkRuleDialog(
    initial: StreamNetworkRule,
    settings: PlayerSettings,
    position: Int,
    total: Int,
    existing: Boolean,
    onSave: (StreamNetworkRule) -> Unit,
    onMove: (StreamNetworkRule, Int) -> Unit,
    onDelete: () -> Unit,
    onDismiss: () -> Unit
) {
    val context = LocalContext.current
    var draft by remember(initial.id) { mutableStateOf(initial) }
    val prefixFocusRequester = remember { FocusRequester() }
    LaunchedEffect(initial.id) { prefixFocusRequester.requestFocusAfterFrames() }

    NuvioDialog(
        onDismiss = onDismiss,
        title = stringResource(R.string.mod_stream_rules_edit_title),
        subtitle = stringResource(R.string.mod_stream_rules_position, position, total),
        width = 640.dp,
        suppressFirstKeyUp = false
    ) {
        NuvioTextField(
            value = draft.urlPrefix,
            onValueChange = { draft = draft.copy(urlPrefix = it) },
            modifier = Modifier.fillMaxWidth(),
            placeholder = stringResource(R.string.mod_stream_rules_prefix_hint),
            focusRequester = prefixFocusRequester
        )
        SettingsToggleRow(
            title = stringResource(R.string.playback_net_auto_speedup),
            subtitle = null,
            checked = draft.autoSpeedup,
            onToggle = { draft = draft.copy(autoSpeedup = !draft.autoSpeedup) }
        )
        SliderSettingsItem(
            title = stringResource(R.string.playback_net_connections),
            values = (StreamNetworkRule.CONNECTIONS_AUTOMATIC..
                MemoryBudget.maxConnections(settings.nuvioPerformanceModeEnabled).coerceAtLeast(2)).toList(),
            selected = draft.connections,
            valueText = connectionsLabel(draft.connections),
            onValueChange = { draft = draft.copy(connections = it) }
        )
        if (draft.connections >= 2) {
            // Same ceiling the global Chunk size slider uses, worked out for this rule's connection count.
            val effective = draft.applyTo(settings)
            val maxChunkMb = if (settings.nuvioPerformanceModeEnabled) {
                MemoryBudget.customMaxChunkMb(context, effective)
            } else {
                MemoryBudget.maxChunkMb(
                    MemoryBudget.effectiveBufferMb(effective.bufferSettings.targetBufferSizeMb),
                    effective.parallelConnectionCount
                )
            }
            val sizes = CHUNK_SIZES_KB.filter { it <= maxChunkMb * 1024 }.ifEmpty { listOf(CHUNK_SIZES_KB.first()) }
            val selected = sizes.lastOrNull { it <= draft.chunkSizeKb } ?: sizes.first()
            SliderSettingsItem(
                title = stringResource(R.string.playback_net_chunk_size),
                values = sizes,
                selected = selected,
                valueText = if (selected < 1024) "$selected KB" else "${selected / 1024} MB",
                onValueChange = { draft = draft.copy(chunkSizeKb = it) }
            )
        }
        if (draft.autoSpeedup && draft.connections != StreamNetworkRule.CONNECTIONS_AUTOMATIC) {
            SettingsNote(text = stringResource(R.string.playback_net_auto_speedup_custom_note))
        }
        if (existing) {
            SettingsDialogActionRow(horizontalAlignment = Alignment.Start) {
                SettingsDialogActionButton(
                    text = stringResource(R.string.mod_stream_rules_move_up),
                    onClick = { onMove(draft, -1) },
                    enabled = position > 1
                )
                SettingsDialogActionButton(
                    text = stringResource(R.string.mod_stream_rules_move_down),
                    onClick = { onMove(draft, 1) },
                    enabled = position < total
                )
                SettingsDialogActionButton(text = stringResource(R.string.action_delete), onClick = onDelete)
            }
        }
        SettingsDialogActionRow {
            SettingsDialogActionButton(text = stringResource(R.string.action_cancel), onClick = onDismiss)
            SettingsDialogActionButton(
                text = stringResource(R.string.action_save),
                onClick = { onSave(draft.copy(urlPrefix = draft.urlPrefix.trim())) },
                primary = true,
                enabled = draft.urlPrefix.isNotBlank()
            )
        }
    }
}

private val CHUNK_SIZES_KB = listOf(256, 512, 1024, 2048, 4096, 8192, 12288, 16384, 24576, 32768, 49152, 65536, 98304, 131072)
