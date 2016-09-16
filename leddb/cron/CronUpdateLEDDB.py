#!/usr/bin/env python
# CRON TASK
# node: node001
# when: daily
# time: 23pm
import os
import ledama.config as lconfig
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule UpdateLEDDB -a')