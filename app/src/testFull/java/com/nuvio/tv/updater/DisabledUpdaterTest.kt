package com.nuvio.tv.updater

import android.app.job.JobScheduler
import android.content.Context
import com.nuvio.tv.BuildConfig
import com.nuvio.tv.core.build.AppFeaturePolicy
import io.mockk.*
import org.junit.Assert.*
import org.junit.Test
import java.io.File

class DisabledUpdaterTest {
    private val scheduler = mockk<JobScheduler>(relaxed = true)
    private val context = mockk<Context> {
        every { getSystemService(Context.JOB_SCHEDULER_SERVICE) } returns scheduler
    }

    @Test fun disabledJobsCancelPendingWorkAndNeverScheduleNewWork() {
        assertFalse(AppFeaturePolicy.inAppUpdatesEnabled)
        assertFalse(BuildConfig.UPDATE_CHECK_ENABLED)
        assertFalse(BuildConfig.FEATURE_IN_APP_UPDATES_ENABLED)
        UpdateJobService.schedulePeriodicCheck(context)
        UpdateJobService.scheduleCheckNow(context)
        UpdateJobService.scheduleIdleInstall(context)
        assertFalse(UpdateJobService.canInstallSilently(context))
        verify(exactly = 0) { scheduler.schedule(any()) }
        verify(exactly = 3) { scheduler.cancel(10020) }
        verify(exactly = 3) { scheduler.cancel(10021) }
        verify(exactly = 3) { scheduler.cancel(10022) }
    }

    @Test fun disabledViewModelDoesNotReadPreferencesCheckDownloadOrInstall() {
        val repository = mockk<UpdateRepository>()
        val preferences = mockk<UpdatePreferences>()
        val store = mockk<UpdateDownloadStore>()
        val model = UpdateViewModel(context, repository, preferences, store)
        model.checkForUpdates(force = true, showNoUpdateFeedback = false)
        model.setUpdateBannerEnabled(true)
        model.setAutoInstallEnabled(true)
        model.downloadUpdate()
        model.installUpdateOrRequestPermission()
        assertFalse(model.uiState.value.updateBannerEnabled)
        assertFalse(model.uiState.value.autoInstallEnabled)
        assertFalse(model.uiState.value.showBanner)
        coVerify { repository wasNot Called }
        coVerify { preferences wasNot Called }
        coVerify { store wasNot Called }
    }

    @Test fun directInstallerEntryPointsCannotLaunchPackageInstaller() {
        val file = File("unused-update.apk")
        assertThrows(IllegalStateException::class.java) { ApkInstaller.install(context, file, false) }
        assertThrows(IllegalStateException::class.java) { ApkInstaller.launchSystemInstaller(context, file) }
        verify(exactly = 0) { context.packageManager }
        verify(exactly = 0) { context.startActivity(any()) }
    }
}
