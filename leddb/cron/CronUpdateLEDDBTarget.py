#!/usr/bin/env python
# CRON TASK
# node: node001
# when: daily
# time: 7am
import os
import ledama.config as lconfig
import ledama.utils as utils

os.system('rm -rf ' + lconfig.TARGET_FDT_UPDATE_FOLDER)
os.system('mkdir -p ' + lconfig.TARGET_FDT_UPDATE_FOLDER)
os.chdir(lconfig.TARGET_FDT_UPDATE_FOLDER)
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule UpdateLEDDBTarget -s ' + utils.TARGET_E_EOR + ' &> ' + lconfig.TARGET_FDT_UPDATE_FOLDER + '/' + utils.TARGET_E_EOR + '.log')
os.system('python ' + lconfig.LEDAMA_ABS_PATH + '/ExecuteLModule UpdateLEDDBTarget -s ' + utils.TARGET_F_EOR + ' &> ' + lconfig.TARGET_FDT_UPDATE_FOLDER + '/' + utils.TARGET_F_EOR + '.log')