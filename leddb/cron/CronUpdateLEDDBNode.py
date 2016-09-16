#!/usr/bin/env python
# CRON TASK
# node: ALL NODES
# when: daily
# time: 00pm (we introduce a sleep time in order we guarantee bunch of 1 hour where only 15 nodes are updating)
import os
import time
import ledama.config as lconfig
hostName = (os.popen("echo $HOSTNAME")).read().split('\n')[0] 
sleepTime = 3600*((int(hostName.replace('node',''))-11) / 15)
if sleepTime < 0:
    sleepTime = 0
time.sleep(sleepTime)
logFile = lconfig.LEDDB_LOGS_FOLDER + '/' + hostName + '.log'
if os.path.isfile(logFile):
    os.system('rm ' + logFile)
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule UpdateLEDDBNode -d -f -p 1  &> ' + logFile)