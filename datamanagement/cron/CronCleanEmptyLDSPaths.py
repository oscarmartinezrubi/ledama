#!/usr/bin/env python
# CRON TASK
# node: node001
# when: monthly (1st of each month)
# time: 8pm
import os
import ledama.config as lconfig
logFile = lconfig.LEDDB_LOGS_FOLDER + '/' + 'CLEAN_EMPTY_PATHS.log'
if os.path.isfile(logFile):
    os.system('rm ' + logFile)
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule CleanEmptyLDSPaths &> ' + logFile)