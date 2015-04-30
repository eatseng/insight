# import zipfile
import sys
import getopt
import subprocess
import traceback

from clay import config
from datetime import datetime
from datamonkey.lib.error import error_email

script_log = config.get_logger('mr_scripts')


def print_help():
    print("mr_runner.py: \n--help/-h : help")
    print("--script/-s : specify a script, ex: -s 'script_file'")
    print("--input/-i: specify a log file")
    print("--output-dir: specify output directory")
    print("--local/--emr: run mr in local or on aws emr")
    print("example: mr_runner -s \"jobs/map_reduce/script.py\" -i \"input_log\" --emr")


class MRJobRunner(object):

    def __init__(self, argv):
        self.scripts = []
        self.output_dir = None
        self.loadcache = None
        self.env = "local"

        self.output, self.error = None, None

        try:
            #white list input options
            opts, args = getopt.getopt(argv, "hs:i:l", ["help", "script=", "input=", "output-dir", "loadcache", "local", "emr"])
        except getopt.GetoptError:
            print_help()
            sys.exit(2)

        for opt, arg in opts:
            if opt in ("-h", "--help"):
                print_help()
                sys.exit()
            elif opt in ("-s", "--script"):
                self.scripts = arg.split()
            elif opt in ("-i", "--input"):
                self.inputs = arg.split()
            elif opt in ("--output-dir"):
                self.output_dir = arg
            elif opt in ("-l", "--loadcache"):
                self.loadcache = True
            elif opt in ("--emr"):
                self.env = "emr"

    def run(self):
        if self.scripts is None or len(self.scripts) == 0 \
                or self.inputs is None or len(self.inputs) == 0:
            print_help()
            sys.exit(2)

        start_time = datetime.utcnow()

        script_log.info("Running MR job: %s" % self.scripts[0].split("/")[-1])

        try:
            if self.output_dir is not None:
                subprocess.Popen(["python", self.scripts[0], "-r", self.env, "--conf-path", "config/mrjob_local.conf", self.inputs[0], "--output-dir", self.output_dir]).wait()

            if self.loadcache is True:
                self.output, self.error = subprocess.Popen(["python", self.scripts[0], "-r", self.env, "--conf-path", "config/mrjob_local.conf", self.inputs[0]], stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            else:
                subprocess.Popen(["python", self.scripts[0], self.env, "--conf-path", "config/mrjob_local.conf", self.inputs[0]]).wait()

        except Exception, e:
            print(e)
            script_log.error("MRJob %s failed: %s" % (self.scripts[0].split("/")[-1], e))
            error_email(table_name=self.scripts[0].split("/")[-1], error=e, trace=(''.join(traceback.format_stack())))

        duration = int((datetime.utcnow() - start_time).total_seconds())

        # print(duration)

    def get_mr_output(self):
        return {"output": self.output, "error": self.error}

# CLAY_CONFIG=./config/prod.yaml python -m jobs.mr_runner -s "jobs/map_reduce/qos_event.py" -i "jobs/fuzehouse/etl_scripts/my_test.py"
# CLAY_CONFIG=./config/prod.yaml python -m jobs.fuzehouse.etl_scripts.my_test -r emr --conf-path config/mrjob_local.conf s3://fuze-fuzehouse-dev/staging/Fuze-4616-2014-11-14-11-00-25.log --output-dir s3://fuze-fuzehouse-dev/logs/some
# CLAY_CONFIG=./config/prod.yaml python -m jobs.mr_runner -s "jobs/map_reduce/qos_event.py" -i "./tmp/logs/" --local --output-dir "tmp/output"
# CLAY_CONFIG=./config/prod.yaml python -m jobs.mr_runner -s "jobs/map_reduce/tweet_count.py" -i "./tmp/logs/" --local --output-dir "tmp/output"


if __name__ == "__main__":
    runner = MRJobRunner(sys.argv[1:])
    runner.run()
