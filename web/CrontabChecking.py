#!/usr/bin/env python
import os
import ledama.config as lconfig
# Count how many times we see the webleddb.py in the current processes of the 
# node (of course we ignore the current grep process)
lines = os.popen('ps aux | grep webleddb.py').read().split('\n')
counter = 0
for line in lines:
    if line != '' and line.count('grep webleddb.py') == 0:
        counter += 1
        
# If no process is detected we restart the web
if counter == 0:
    os.chdir('/home/users/leddbweb/web')
    os.system('python webleddb.py &> /home/users/leddbweb/web/cron.log &')