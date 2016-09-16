#!/usr/bin/env python
# CRON TASK
# node: node080
# when: weekly (every saturday)
# time: 9 am
import os
import ledama.config as lconfig
logFile = lconfig.LEDDB_LOGS_FOLDER + '/' + 'BACKUP.log'
if os.path.isfile(logFile):
    os.system('rm ' + logFile)
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule BackupLEDDB -b -v -f -r -d ' + lconfig.LEDDB_BACKUP_FOLDER + ' &> ' + logFile)