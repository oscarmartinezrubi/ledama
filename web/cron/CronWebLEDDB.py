#!/usr/bin/env python
# CRON TASK
# node: node078 (with user leddbweb)
# when: daily
# time: at reboot and every 10 minutes
import os
from ledama import config as lconfig
# Count how many times we see the webleddb.py in the current processes of the 
# node (of course we ignore the current grep process)
lines = os.popen('ps aux | grep webleddb.py').read().split('\n')
counter = 0
for line in lines:
    if line != '' and line.count('grep webleddb.py') == 0:
        counter += 1
        
# If no process is detected we restart the web
if counter == 0:
    os.chdir(lconfig.LEDDB_WEB_DIR)
    os.system('python webleddb.py &> ' + lconfig.LEDDB_WEB_DIR + '/cron.log &')