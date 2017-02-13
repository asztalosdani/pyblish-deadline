import os
import sys
import logging
import json

import Deadline.Events
import Deadline.Scripting as ds


def GetDeadlineEventListener():
    return PyblishEventListener()


def CleanupDeadlineEventListener(eventListener):
    eventListener.Cleanup()


class PyblishEventListener(Deadline.Events.DeadlineEventListener):

    def __init__(self):
        self.OnJobSubmittedCallback += self.OnJobSubmitted
        self.OnJobStartedCallback += self.OnJobStarted
        self.OnJobFinishedCallback += self.OnJobFinished
        self.OnJobRequeuedCallback += self.OnJobRequeued
        self.OnJobFailedCallback += self.OnJobFailed
        self.OnJobSuspendedCallback += self.OnJobSuspended
        self.OnJobResumedCallback += self.OnJobResumed
        self.OnJobPendedCallback += self.OnJobPended
        self.OnJobReleasedCallback += self.OnJobReleased
        self.OnJobDeletedCallback += self.OnJobDeleted
        self.OnJobErrorCallback += self.OnJobError
        self.OnJobPurgedCallback += self.OnJobPurged

        self.OnHouseCleaningCallback += self.OnHouseCleaning
        self.OnRepositoryRepairCallback += self.OnRepositoryRepair

        self.OnSlaveStartedCallback += self.OnSlaveStarted
        self.OnSlaveStoppedCallback += self.OnSlaveStopped
        self.OnSlaveIdleCallback += self.OnSlaveIdle
        self.OnSlaveRenderingCallback += self.OnSlaveRendering
        self.OnSlaveStartingJobCallback += self.OnSlaveStartingJob
        self.OnSlaveStalledCallback += self.OnSlaveStalled

        self.OnIdleShutdownCallback += self.OnIdleShutdown
        self.OnMachineStartupCallback += self.OnMachineStartup
        self.OnThermalShutdownCallback += self.OnThermalShutdown
        self.OnMachineRestartCallback += self.OnMachineRestart

    def Cleanup(self):
        del self.OnJobSubmittedCallback
        del self.OnJobStartedCallback
        del self.OnJobFinishedCallback
        del self.OnJobRequeuedCallback
        del self.OnJobFailedCallback
        del self.OnJobSuspendedCallback
        del self.OnJobResumedCallback
        del self.OnJobPendedCallback
        del self.OnJobReleasedCallback
        del self.OnJobDeletedCallback
        del self.OnJobErrorCallback
        del self.OnJobPurgedCallback

        del self.OnHouseCleaningCallback
        del self.OnRepositoryRepairCallback

        del self.OnSlaveStartedCallback
        del self.OnSlaveStoppedCallback
        del self.OnSlaveIdleCallback
        del self.OnSlaveRenderingCallback
        del self.OnSlaveStartingJobCallback
        del self.OnSlaveStalledCallback

        del self.OnIdleShutdownCallback
        del self.OnMachineStartupCallback
        del self.OnThermalShutdownCallback
        del self.OnMachineRestartCallback

    def run_pyblish(self, config_entry, job, additonalData={}):

        plugin_dir = ds.RepositoryUtils.GetEventPluginDirectory("Pyblish")

        # Activating pre and post task scripts, if paths are configured.
        if config_entry == "OnJobSubmittedPaths":
            if self.GetConfigEntryWithDefault("OnPostTaskPaths", ""):
                path = os.path.join(plugin_dir, "OnPostTask.py")
                if os.path.exists(path):
                    job.JobPostTaskScript = path
                    self.LogInfo("Adding OnPostTask: " + path)
            if self.GetConfigEntryWithDefault("OnPreTaskPaths", ""):
                path = os.path.join(plugin_dir, "OnPreTask.py")
                if os.path.exists(path):
                    job.JobPreTaskScript = path
                    self.LogInfo("Adding OnPreTask: " + path)

        ds.RepositoryUtils.SaveJob(job)

        # Setup environment
        PYTHONPATH = ""
        if job.GetJobEnvironmentKeys():
            self.LogInfo("Getting environment from job:")
            for key in job.GetJobEnvironmentKeys():
                value = job.GetJobEnvironmentKeyValue(key)
                os.environ[str(key)] = str(value)
                self.LogInfo("{0}={1}".format(key, value))
                if str(key) == "PYTHONPATH":
                    PYTHONPATH = str(value)

        # Adding python search paths.
        paths = self.GetConfigEntryWithDefault("PythonSearchPaths", "").strip()
        paths = paths.split(";")
        paths += PYTHONPATH.split(os.pathsep)

        for path in paths:
            self.LogInfo("Extending sys.path with: " + str(path))
            sys.path.append(path)

        # Clearing previous plugin paths,
        # and adding pyblish plugin search paths.
        os.environ["PYBLISHPLUGINPATH"] = ""
        path = ""
        adding_paths = self.GetConfigEntryWithDefault(config_entry, "").strip()
        adding_paths += os.pathsep + os.environ.get(config_entry, "")

        # Return early if no plugins were found.
        if adding_paths == os.pathsep:
            self.LogInfo("No plugins found.")
            return
        else:
            adding_paths.replace(";", os.pathsep)

            if path != "":
                path = path + os.pathsep + adding_paths
            else:
                path = adding_paths

            self.LogInfo("Setting PYBLISHPLUGINPATH to: \"%s\"" % path)
            os.environ["PYBLISHPLUGINPATH"] = str(path)

        # Setup logging.
        level_item = self.GetConfigEntryWithDefault("LoggingLevel", "DEBUG")
        level = logging.DEBUG

        if level_item == "INFO":
            level = logging.INFO
        if level_item == "WARNING":
            level = logging.WARNING
        if level_item == "ERROR":
            level = logging.ERROR

        logging.basicConfig(level=level)
        logger = logging.getLogger()

        # If pyblish is not available.
        try:
            __import__("pyblish.api")
        except ImportError:
            import traceback
            print ("Could not load module \"pyblish.api\": %s"
                   % traceback.format_exc())
            return

        import pyblish.api

        # Register host
        pyblish.api.register_host("deadline")

        # Setup context and injecting deadline job and additional data.
        cxt = pyblish.api.Context()

        cxt.data["deadlineJob"] = job
        cxt.data["deadlineAdditionalData"] = additonalData

        # Recreate context from data.
        data = job.GetJobExtraInfoKeyValueWithDefault("PyblishContextData", "")
        if data:
            data = json.loads(data)
            cxt.data.update(data)
        else:
            logger.warning("No Pyblish data found.")

        cxt.data["deadlineEvent"] = config_entry.replace("Paths", "")

        # Run publish.
        import pyblish.util

        logging.getLogger("pyblish").setLevel(level)

        cxt = pyblish.util.publish(context=cxt)

        # Error logging needs some work.
        for result in cxt.data["results"]:
            if not result["success"]:
                logger.error(result)
                (file_path, line_no, func, line) = result["error"].traceback
                msg = "Error: \"{0}\"\n".format(result["error"])
                msg += "Filename: \"{0}\"\n".format(file_path)
                msg += "Line number: \"{0}\"\n".format(line_no)
                msg += "Function name: \"{0}\"\n".format(func)
                msg += "Line: \"{0}\"\n".format(line)
                logger.error(msg)

    def OnJobSubmitted(self, job):

        self.run_pyblish("OnJobSubmittedPaths", job)

    def OnJobStarted(self, job):

        self.run_pyblish("OnJobStartedPaths", job)

    def OnJobFinished(self, job):

        self.run_pyblish("OnJobFinishedPaths", job)

    def OnJobRequeued(self, job):

        self.run_pyblish("OnJobRequeuedPaths", job)

    def OnJobFailed(self, job):

        self.run_pyblish("OnJobFailedPaths", job)

    def OnJobSuspended(self, job):

        self.run_pyblish("OnJobSuspendedPaths", job)

    def OnJobResumed(self, job):

        self.run_pyblish("OnJobResumedPaths", job)

    def OnJobPended(self, job):

        self.run_pyblish("OnJobPendedPaths", job)

    def OnJobReleased(self, job):

        self.run_pyblish("OnJobReleasedPaths", job)

    def OnJobDeleted(self, job):

        self.run_pyblish("OnJobDeletedPaths", job)

    def OnJobError(self, job, task, report):

        data = {"task": task, "report": report}
        self.run_pyblish("OnJobErrorPaths", job, data)

    def OnJobPurged(self, job):

        self.run_pyblish("OnJobPurgedPaths", job)

    def OnHouseCleaning(self):

        self.run_pyblish("OnHouseCleaningPaths", None)

    def OnRepositoryRepair(self, job):

        self.run_pyblish("OnRepositoryRepairPaths", job)

    def OnSlaveStarted(self, job):

        self.run_pyblish("OnSlaveStartedPaths", job)

    def OnSlaveStopped(self, job):

        self.run_pyblish("OnSlaveStoppedPaths", job)

    def OnSlaveIdle(self, job):

        self.run_pyblish("OnSlaveIdlePaths", job)

    def OnSlaveRendering(self, slaveName, job):

        self.run_pyblish("OnSlaveRenderingPaths", job)

    def OnSlaveStartingJob(self, slaveName, job):

        self.run_pyblish("OnSlaveStartingJobPaths", job)

    def OnSlaveStalled(self, job):

        self.run_pyblish("OnSlaveStalledPaths", job)

    def OnIdleShutdown(self, job):

        self.run_pyblish("OnIdleShutdownPaths", job)

    def OnMachineStartup(self, job):

        self.run_pyblish("OnMachineStartupPaths", job)

    def OnThermalShutdown(self, job):

        self.run_pyblish("OnThermalShutdownPaths", job)

    def OnMachineRestart(self, job):

        self.run_pyblish("OnMachineRestartPaths", job)
