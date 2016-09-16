#!/usr/bin/env python
# CRON TASK
# node: node001
# when: Wednesdays
# time: 23pm
import os
import ledama.config as lconfig
logFile = lconfig.LEDDB_LOGS_FOLDER + '/' + 'CLEAN.log'
if os.path.isfile(logFile):
    os.system('rm ' + logFile)
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule RemoveFromLEDDB -c &> ' + logFile)