#!/usr/bin/env python
# CRON TASK
# node: ALL NODES
# when: daily
# time: at reboot and every hour
import subprocess
from ledama import config as lconfig
from ledama import utils
from ledama.nodes.NodeMonitorDaemon import getOutFilePath, getErrFilePath
# we restart the daemon every hour
command = 'python ' + lconfig.LEDAMA_ABS_PATH + '/nodes/NodeMonitorDaemon.py'
hostName = utils.getHostName()
subprocess.Popen(command + ' stop', shell = True, stdout=open(getOutFilePath(hostName),'w'), stderr=open(getErrFilePath(hostName),'w')).communicate()
subprocess.Popen(command + ' start', shell = True, stdout=open(getOutFilePath(hostName),'w'), stderr=open(getErrFilePath(hostName),'w')).communicate()