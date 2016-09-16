#!/usr/bin/env python
# CRON TASK
# node: node001
# when: daily
# time: 8am
import os
import ledama.config as lconfig
logFile = lconfig.LEDDB_LOGS_FOLDER + '/' + 'META.log'
if os.path.isfile(logFile):
    os.system('rm ' + logFile)
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule UpdateLEDDBMeta -n &> ' + logFile)