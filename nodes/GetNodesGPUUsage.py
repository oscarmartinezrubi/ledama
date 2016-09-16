#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, json
from ledama.nodes.NodeMonitorDaemon import *
import ledama.utils as utils
import ledama.config as lconfig
import ledama.leddb.LEDDBOps as LEDDBOps
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *

def getUsageSnapshot(node):
        return os.popen('ssh -o NumberOfPasswordPrompts=0 ' + node + ' "nvidia-smi -a"').read().split('\n')
    
# Get the gpu usages dictionary (keys are the nodes)
def getGPUUsageDictionary(nodes, gpus):
    
    
    if lconfig.NODE_MONITOR_TYPE == 'db':
        connection = Connector(lconfig.NODE_MONITOR_DB_NAME, utils.getUserName(), lconfig.NODE_MONITOR_DB_HOST).getConnection()
    
    dictionary = {}
    for node in nodes:
        dictionary[node] = []    
        # Initialize the elements of the dictionary for all the required keys
        for j in range(len(gpus)):
            dictionary[node].append((-1,-1))
        # Now we try to get the data from the gpu usage file generated with the 
        # daemon and we also check that the time stamp is not too old (older than 100 s)
        try:
            if lconfig.NODE_MONITOR_TYPE == 'db':
                loadedData = json.loads(LEDDBOps.select(connection, HOST, {NAME:node}, [GPUUSAGEMON,],)[0][0])
            else:
                f = open(getGPUUsageFilePath(node), 'rb')
                loadedData = pickle.load(f)
                f.close()
            isTooOld = utils.isTimeStampOlder(loadedData.pop('CURRENTTIME'), utils.getCurrentTimeStamp(), 500) 
        except:
            isTooOld = True
            
        if not isTooOld:
            for j in range(len(gpus)):
                if j in loadedData:
                    dictionary[node][j] = loadedData[j]
    return dictionary
    
    
    