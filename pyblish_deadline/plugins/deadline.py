import os
import subprocess
import tempfile
import traceback
import re
import uuid
import json
from collections import defaultdict

import pyblish.api


class IntegrateDeadline(pyblish.api.ContextPlugin):

    label = "Deadline Submission"
    order = pyblish.api.IntegratorOrder
    optional = True

    def process(self, context):

        self.orders = []
        jobs_entities_by_order = defaultdict(list)
        jobs_entities_no_order = []

        for instance in context:

            if not instance.data.get("publish", True):
                continue

            # skipping instance if not part of the family
            if "deadline" not in instance.data.get("families", []):
                msg = "No \"deadline\" family assigned. "
                msg += "Skipping \"%s\"." % instance
                self.log.info(msg)
                continue

            jobs = instance.data("deadlineData")
            # maintain backward compatibility, where only job could be submitted per instance
            if not isinstance(jobs, list):
                jobs = [jobs]

            for job in jobs:
                if "order" in job:
                    order = job["order"]
                    self.orders.append(order)
                    jobs_entities_by_order[order].append((job, instance))
                else:
                    jobs_entities_no_order.append((job, instance))

        if "deadlineData" in context.data:
            jobs = context.data("deadlineData")
            if not isinstance(jobs, list):
                jobs = [jobs]

            for job in jobs:
                if "order" in job:
                    order = job["order"]
                    self.orders.append(order)
                    jobs_entities_by_order[order].append((job, context))
                else:
                    jobs_entities_no_order.append((job, context))

        self.orders = list(set(self.orders))
        self.orders.sort()

        jobs_entities_sorted = []
        for order in self.orders:
            for job_instance in jobs_entities_by_order[order]:
                jobs_entities_sorted.append(job_instance)

        jobs_entities_sorted.extend(jobs_entities_no_order)

        self.job_ids = [[] for i in self.orders]
        for job, instance in jobs_entities_sorted:
            self._process_job(job, instance)

    def _process_job(self, job, entity):
            submission_id = uuid.uuid4()

            # getting job data
            job_data = job["job"]

            if isinstance(entity, pyblish.api.Context):
                context = entity
            elif isinstance(entity, pyblish.api.Instance):
                instance = entity
                context = instance.context
                # setting instance data
                data = {}
                for key in instance.data:
                    try:
                        json.dumps(instance.data[key])
                        data[key] = instance.data[key]
                    except:
                        msg = "\"{0}\"".format(instance.data[key])
                        msg += " in instance.data[\"{0}\"]".format(key)
                        msg += " could not be serialized."
                        self.log.warning(msg)
                data = json.dumps(data)

                if "ExtraInfoKeyValue" in job_data:
                    job_data["ExtraInfoKeyValue"]["PyblishInstanceData"] = data
                else:
                    job_data["ExtraInfoKeyValue"] = {"PyblishInstanceData": data}
            else:
                self.log.warning("Unsupported type: ", type(entity))
                return

            # setting context data
            context_data = context.data.copy()
            del context_data["results"]
            if "deadlineJob" in context_data:
                del context_data["deadlineJob"]

            data = {}
            for key in context_data:
                try:
                    json.dumps(context_data[key])
                    data[key] = context_data[key]
                except:
                    msg = "\"{0}\"".format(context_data[key])
                    msg += " in context.data[\"{0}\"]".format(key)
                    msg += " could not be serialized."
                    self.log.warning(msg)
            data = json.dumps(data)

            if "ExtraInfoKeyValue" in job_data:
                job_data["ExtraInfoKeyValue"]["PyblishContextData"] = data
            else:
                job_data["ExtraInfoKeyValue"] = {"PyblishContextData": data}

            # setting up dependencies
            if "order" in job:
                order = job["order"]
                current_order_index = self.orders.index(order)
                if current_order_index != 0:
                    index = current_order_index - 1
                    dependencies = self.job_ids[index]
                    for i, job_id in enumerate(dependencies):
                        name = "JobDependency%s" % i
                        job_data[name] = job_id

            # writing job data
            data = ""

            if "ExtraInfo" in job_data:
                for v in job_data["ExtraInfo"]:
                    index = job_data["ExtraInfo"].index(v)
                    data += "ExtraInfo%s=%s\n" % (index, v)
                del job_data["ExtraInfo"]

            if "ExtraInfoKeyValue" in job_data:
                index = 0
                for entry in job_data["ExtraInfoKeyValue"]:
                    data += "ExtraInfoKeyValue%s=" % index
                    data += "%s=" % entry
                    data += "%s\n" % job_data["ExtraInfoKeyValue"][entry]
                    index += 1
                del job_data["ExtraInfoKeyValue"]

            if "EnvironmentKeyValue" in job_data:
                index = 0
                for entry in job_data["EnvironmentKeyValue"]:
                    data += "EnvironmentKeyValue%s=" % index
                    data += "%s=" % entry
                    data += "%s\n" % job_data["EnvironmentKeyValue"][entry]
                    index += 1
                del job_data["EnvironmentKeyValue"]

            for entry in job_data:
                data += "%s=%s\n" % (entry, job_data[entry])

            current_dir = tempfile.gettempdir()
            filename = str(submission_id) + ".job.txt"
            job_path = os.path.join(current_dir, filename)

            with open(job_path, "w") as outfile:
                outfile.write(data)

            self.log.info("job data:\n\n%s" % data)

            # writing plugin data
            plugin_data = job["plugin"]
            data = ""
            for entry in plugin_data:
                data += "%s=%s\n" % (entry, plugin_data[entry])

            current_dir = tempfile.gettempdir()
            filename = str(submission_id) + ".plugin.txt"
            plugin_path = os.path.join(current_dir, filename)

            with open(plugin_path, "w") as outfile:
                outfile.write(data)

            self.log.info("plugin data:\n\n%s" % data)

            # submitting job
            args = [job_path, plugin_path]

            if "auxiliaryFiles" in entity.data["deadlineData"]:
                aux_files = job["auxiliaryFiles"]
                if isinstance(aux_files, list):
                    args.extend(aux_files)
                else:
                    args.append(aux_files)

            # submitting
            try:
                result = self.CallDeadlineCommand(args)

                self.log.info(result)

                job_id = re.search(r"JobID=(.*)", result).groups()[0]

                if "order" in job:
                    order = job["order"]
                    index = self.orders.index(order)
                    self.job_ids[index].append(job_id)
            except:
                raise ValueError(traceback.format_exc())

            # deleting temporary files
            os.remove(job_path)
            os.remove(plugin_path)

    def CallDeadlineCommand(self, arguments, hideWindow=True):
        # On OSX, we look for the DEADLINE_PATH file. On other platforms,
        # we use the environment variable.
        if os.path.exists("/Users/Shared/Thinkbox/DEADLINE_PATH"):
            with open("/Users/Shared/Thinkbox/DEADLINE_PATH") as f:
                deadlineBin = f.read().strip()
                deadlineCommand = deadlineBin + "/deadlinecommand"
        else:
            deadlineBin = os.environ["DEADLINE_PATH"]
            if os.name == "nt":
                deadlineCommand = deadlineBin + "\\deadlinecommand.exe"
            else:
                deadlineCommand = deadlineBin + "/deadlinecommand"

        startupinfo = None
        if hideWindow and os.name == "nt" and hasattr(subprocess,
                                                      "STARTF_USESHOWWINDOW"):
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW

        environment = {}
        for key in os.environ.keys():
            environment[key] = str(os.environ[key])

        # Need to set the PATH, cuz windows seems to load DLLs from the PATH
        # earlier that cwd....
        if os.name == "nt":
            path = str(deadlineBin + os.pathsep + os.environ["PATH"])
            environment["PATH"] = path

        arguments.insert(0, deadlineCommand)

        # Specifying PIPE for all handles to
        # workaround a Python bug on Windows.
        # The unused handles are then closed immediatley afterwards.
        proc = subprocess.Popen(arguments, cwd=deadlineBin,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                startupinfo=startupinfo,
                                env=environment)
        proc.stdin.close()
        proc.stderr.close()

        output = proc.stdout.read()

        return output
