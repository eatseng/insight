from __future__ import absolute_import

import importlib
import time

PATHS = ['jobs.insighthouse.eb_loads.eb_daily_events'
         ]

if __name__ == "__main__":
    for path in PATHS:
        job_mod = importlib.import_module(path)
        job = getattr(job_mod, 'job')()
        job.run()
        time.sleep(30)
