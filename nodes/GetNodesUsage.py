#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, json
import cPickle as pickle
from ledama.nodes.NodeMonitorDaemon import *
import ledama.utils as utils
import ledama.config as lconfig
import ledama.leddb.LEDDBOps as LEDDBOps
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *

def getUsageSnapshot(node, column, userName):
    lines = []
    lines.extend(os.popen("ssh " + node + " top -b -n 1 | head -7").read().split('\n'))
    
    if column == 'CPU':
        aux = '9'
    else:
        aux = '10'
    
    if userName == '':
        aux = os.popen("ssh " + node + " top -b -n 1 | tail -n +8 | sort -nr -k " + aux + "," + aux).read().split('\n')
        if len(aux):
            lines.extend(aux)
    else:
        aux = (os.popen("ssh " + node + " top -b -n 1 | tail -n +8 | grep " + userName + " | sort -nr -k " + aux+ "," + aux).read().split('\n'))   
        if len(aux):
            lines.extend(aux)
    return lines

# Get the usage dictionary from the Daemon, the keys are the nodes. Each element 
# is a tuple with the (cpuOfUser,totalCpu,memOfUser,totalMem)
# users array is used to update new users found in the nodes
def getUsageDictionary(nodes, users, commands, user = None, command = None):
    
    if lconfig.NODE_MONITOR_TYPE == 'db':
        connection = Connector(lconfig.NODE_MONITOR_DB_NAME, utils.getUserName(), lconfig.NODE_MONITOR_DB_HOST).getConnection()
        
    dictionary = {}
    for node in nodes:
        dictionary[node] = (-1,-1,-1,-1)
        try:
            if lconfig.NODE_MONITOR_TYPE == 'db':
                loadedData = json.loads(LEDDBOps.select(connection, HOST, {NAME:node}, [USAGEMON,],)[0][0])
            else:
                f = open(getUsageFilePath(node), 'rb')
                loadedData = pickle.load(f)
                f.close()
            isTooOld = utils.isTimeStampOlder(loadedData.pop('CURRENTTIME'), utils.getCurrentTimeStamp(), 100) 
        except:
            isTooOld = True
            
        if not isTooOld:
            myCpu = 0.
            totalCpu = 0.
            myMem = 0.
            totalMem = 0.
            for userKey in loadedData:
                #Update users
                if userKey not in users:
                    users.append(userKey)
                userRecords = loadedData[userKey]
                for userRecord in userRecords:
                    (pid,ucommand,ucpu,umem) = userRecord
                    
                    # Update the totals
                    totalCpu += ucpu
                    totalMem += umem
                    
                    if user == None or user == userKey:
                        # Update the commands ()
                        if ucommand not in commands:
                            commands.append(ucommand)
                    
                        if user == userKey:
                            if command == None or (command != None and command == ucommand):
                                myCpu += ucpu
                                myMem += umem
            dictionary[node] = (myCpu, totalCpu, myMem, totalMem)
    
    return dictionary