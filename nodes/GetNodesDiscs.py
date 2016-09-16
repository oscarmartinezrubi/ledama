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

# Get the content of pathToCheck and return arrays with the paths and sizes
# Folders called with pipeline and backup are recursively checked
def getPathContent(node, pathToCheck):
    
        lines  = os.popen('ssh -o NumberOfPasswordPrompts=0 ' + node + ' "du -chs ' + pathToCheck + '/*"').read().split('\n')
        
        paths = []
        sizes = []
        for line in lines:
            if line != '' and len(line.split()):
                sizes.append(line.split()[0])
                paths.append(line.split()[1])
                if line.count('pipeline') > 0:
                    linespipe = os.popen('ssh -o NumberOfPasswordPrompts=0 ' + node + ' "du -chs ' + pathToCheck + '/pipeline/*"').read().split('\n')
                    if (len(linespipe) > 1):
                        for linepipe in linespipe:
                            if linepipe != '' and len(linepipe.split()) and not linepipe.count('total'): 
                                sizes.append(linepipe.split()[0])
                                paths.append('   ' + linepipe.split()[1])
                        
                if line.count('backup') > 0:
                    linesbackup = os.popen('ssh -o NumberOfPasswordPrompts=0 ' + node + ' "du -chs ' + pathToCheck + '/backup/*"').read().split('\n')
                    if (len(linesbackup) > 1):
                        for linebackup in linesbackup:
                            if linebackup != '' and len(linebackup.split()) and not linebackup.count('total'):
                                sizes.append(linebackup.split()[0])
                                paths.append('   ' + linebackup.split()[1])
                                if linebackup.count('pipeline') > 0: 
                                    linesbackuppipeline = os.popen('ssh -o NumberOfPasswordPrompts=0 ' + node + ' "du -chs ' + pathToCheck + '/backup/pipeline/*"').read().split('\n')
                                    if (len(linesbackuppipeline) > 1):
                                        for linebackuppipeline in linesbackuppipeline:
                                            if linebackuppipeline != '' and len(linebackuppipeline.split()) and not linebackuppipeline.count('total'): 
                                                sizes.append(linebackuppipeline.split()[0])
                                                paths.append('      ' + linebackuppipeline.split()[1])
                                            
        return (paths,sizes)

# Get the storage dictionary (keys are the nodes)
def getStorageDictionary(nodes, mountPoints, delay = 3600):
        
    if lconfig.NODE_MONITOR_TYPE == 'db':
        connection = Connector(lconfig.NODE_MONITOR_DB_NAME, utils.getUserName(), lconfig.NODE_MONITOR_DB_HOST).getConnection()
    
    dictionary = {}
    for node in nodes:
        dictionary[node] = []    
        # Initialize the elements of the dictionary for all the required keys
        for j in range(len(mountPoints)):
            dictionary[node].append((-1,'*'))
        # Now we try to get the data from the storage file generated (or from LEDDB) with the daemon 
        # and we also check that the time stamp is not too old (older than 3600 s)
        try:
            if lconfig.NODE_MONITOR_TYPE == 'db':
                lines = LEDDBOps.select(connection, HOST, {NAME:node}, [STORAGEMON,],)[0][0].split('\n')
            else:
                f = open(getStorageFilePath(node), 'r')
                lines = f.read().split('\n')
                f.close()
            isTooOld = utils.isTimeStampOlder(lines[0], utils.getCurrentTimeStamp(), delay)
        except:
            lines = []
            isTooOld = True
        if len(lines) and not isTooOld:
            # We get and fill the information for the several mount points of 
            # the node
            dictionaryFromFile = {}
            for line in lines:
                fields = line.split()
                if len(fields) == 3:
                    mountFromFile = fields[0]
                    dictionaryFromFile[mountFromFile] = (int(fields[1]),fields[2])
            # We only retain the information related to the required mountPoints
            for j in range(len(mountPoints)):
                if mountPoints[j] in dictionaryFromFile.keys():
                    dictionary[node][j] = dictionaryFromFile[mountPoints[j]]
    
    return dictionary
    
    
    