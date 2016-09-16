#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama.nodes.NodeMonitorDaemon import *
import ledama.utils as utils
import ledama.config as lconfig
import ledama.leddb.LEDDBOps as LEDDBOps
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *

def getNetSnapshot(node, interface):
    
    interval = 2
    
    lines = os.popen("cat /proc/net/dev | grep eth | cut -d ':' -f 1")
    
    lines = os.popen('ssh ' + node + ' \'cat /proc/net/dev;sleep ' + str(interval) + ';cat /proc/net/dev\' | grep ' + interface + ' | cut -d \':\' -f 2 | awk \'{print $1, $9}\'').read().split('\n')

    try:
        velrx = int((float(lines[1].split()[0]) - float(lines[0].split()[0])) / interval)
        veltx = int((float(lines[1].split()[1]) - float(lines[0].split()[1])) / interval)
    except:
        velrx = 0
        veltx = 0
    return interface + ' ' + node + ' ' + str(velrx) + ' ' + str(veltx) + '\n'


def getNetDictionary(interfacesListNodesDictionary):
    
    if lconfig.NODE_MONITOR_TYPE == 'db':
        connection = Connector(lconfig.NODE_MONITOR_DB_NAME, utils.getUserName(), lconfig.NODE_MONITOR_DB_HOST).getConnection()
    
    dictionary = {}
    for node in interfacesListNodesDictionary:
        try:
            if lconfig.NODE_MONITOR_TYPE == 'db':
                lines = LEDDBOps.select(connection, HOST, {NAME:node}, [NETMON,],)[0][0].split('\n')
            else:
                f = open(getNetFilePath(node), 'r')
                lines = f.read().split('\n')
                f.close()
            isTooOld = utils.isTimeStampOlder(lines[0], utils.getCurrentTimeStamp(), 100)
        except:
            lines = []
            isTooOld = True
        interaceDict = {}
        if len(lines) and not isTooOld:
            for line in lines:
                fields = line.split(' ')
                if len(fields) == 3:
                    interaceDict[fields[0]] = (int(fields[1]),int(fields[2]))
            
        for interface in interfacesListNodesDictionary[node]:
            key = (node,interface)
            dictionary[key] = (-1,-1)
            if interface in interaceDict:
                try:
                    dictionary[key] = interaceDict[interface]
                except:
                    continue
    return dictionary
    
    
    
    
    
    
    