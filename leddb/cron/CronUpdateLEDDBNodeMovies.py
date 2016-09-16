#!/usr/bin/env python
# CRON TASK
# node: node007
# when: daily
# time: 7am
import os
import ledama.config as lconfig
logFile = lconfig.LEDDB_LOGS_FOLDER + '/' + 'MOVIES.log'
if os.path.isfile(logFile):
    os.system('rm ' + logFile)
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule UpdateLEDDBNodeMovies -p 16 &> ' + logFile)