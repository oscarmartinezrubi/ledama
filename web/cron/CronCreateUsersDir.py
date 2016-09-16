#!/usr/bin/env python
# CRON TASK
# node: node078 (with user leddbweb)
# when: daily
# time: at midnight
import os
from ledama import config as lconfig
os.system('rm -r ' + lconfig.LEDDB_WEB_DIR + '/users; mkdir -p ' + lconfig.LEDDB_WEB_DIR + '/users; chmod a+r ' + lconfig.LEDDB_WEB_DIR + '/users')
