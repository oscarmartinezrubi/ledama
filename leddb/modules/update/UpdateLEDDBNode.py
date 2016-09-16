################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import time, os
from ledama import utils
from ledama import config as lconfig
from ledama import msoperations
from ledama import nodeoperations
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.MSP import MSP as MSPClass
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.Naming import *
from ledama.leddb import LEDDBOps
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater

class UpdateLEDDBNode(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('path','i','Path[s]',default='/data1/users/lofareor,/data1/users/lofareor/pipeline,/data1/users/lofareor/backup,/data1/users/lofareor/backup/pipeline,/data2/users/lofareor,/data2/users/lofareor/pipeline,/data2/users/lofareor/backup,/data2/users/lofareor/backup/pipeline,/data3/users/lofareor,/data3/users/lofareor/pipeline,/data3/users/lofareor/backup,/data3/users/lofareor/backup/pipeline', helpmessage=' (if multiple, comma-separated no blank spaces), for each one of the provided paths we will list only LDS paths, i.e. the sub-paths L*')
        options.add('usedb','u','Use the LEDDB to get the paths to check',default=False, helpmessage='. If selected the paths indicated with -i will be ignored.')
        options.add('notclean','c','Do not clean the MSP records in the LEDDB',default=False)
        options.add('diagnostic','d','Update also the diagnostic data',default=False)
        options.add('cron','f','Cron task ?',default=False, helpmessage='. It will only update the node if a cron task has been previously setup')
        options.add('processused','g','Process MS being used',default=False, helpmessage='(this may cause threads get stuck in table locks)')
        options.add('numprocessors','p','Simultaneous processors per node',default=1)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)
        # the information
        information = 'update the LEDDB for current node'
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        ldsPath = what
        connection = Connector(self.dbName, self.dbUser , self.dbHost).getConnection()
        updater = MSPUpdater(connection) 
        diagUpdater = DiagnosticUpdater(connection)
        # Get the MS paths within the provided path
        totalCounter = 0
        newMSPsCounter = 0
        errorCounter = 0
        updatedGainsCounter = 0
        updatedQualityCounter = 0
        for absPath in nodeoperations.getMSsFromPath(ldsPath):
            totalCounter += 1
            msp = MSPClass(absPath, self.hostName, msoperations.getSize(absPath))
            if (self.processused) or msp.tar or msp.bvf or (msoperations.isUnused(absPath)):
                try:
                    # Load the Referencing data for this MSP
                    msp.loadReferencingData()
                    # Process it
                    if updater.process(msp):
                        newMSPsCounter += 1
                    if self.diagnostic:
                        (hasGain,isGainUpdated) = diagUpdater.updateGain(msp) 
                        (hasQuality, isQualityUpdated) = diagUpdater.updateQuality(msp)                        
                        # Update the Meta information of this ms
                        diagUpdater.updateMeta(msp.msId, True, hasGain, hasQuality)
                        
                        if isGainUpdated:
                            updatedGainsCounter += 1
                        if isQualityUpdated:
                            updatedQualityCounter += 1
                except Exception,e:
                    print 'Error in ' +  msp.absPath
                    errorCounter += 1
                    errorMessage = str(e)
                    if len(errorMessage) > 250:
                        errorMessage = errorMessage[:120] + ' .... ' + errorMessage[-120:]
                    print errorMessage
            elif not self.processused:
                errorCounter += 1
                print 'Error in ' +  msp.absPath + ': It is being used!!'
        connection.close()
        return (errorCounter, totalCounter, newMSPsCounter, updatedGainsCounter, updatedQualityCounter)
    
    def process(self, path, usedb, notclean, diagnostic, cron, processused, numprocessors, dbname, dbuser, dbhost):
        # Store refercnes to used variables
        self.diagnostic = diagnostic
        self.dbName = dbname
        self.dbUser = dbuser
        self.dbHost = dbhost
        self.processused = processused
        self.hostName = utils.getHostName()
        # Get current time stamp, all the updated measurement sets will have a 
        # higher time stamp
        currentTimeStamp = utils.getCurrentTime() 
        
        # Get from the LEDDB if this node requires an update (in case it is not drectly crond)
        if cron:
            connection = Connector(dbname, dbuser, dbhost).getConnection()
            requiresUpdate = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(connection, HOST, {NAME:self.hostName,}, None, [REQUIRESUPDATE,], True))
            connection.close()
        else:
            requiresUpdate = True
        
        stats = ''
        if not requiresUpdate:
            print utils.getCurrentTimeStamp() + ' - Updating is not required'
        else:    
            
            tstart = time.time()
            print utils.getCurrentTimeStamp() + ' - starting LEDDB node updater in ' + self.hostName + '...'
            if cron:
                # Open a new connection for last update
                connection = Connector(dbname, dbuser, dbhost).getConnection()
                # Set the requiresUpdate to False in order the next checking detects it
                LEDDBOps.update(connection, HOST, {REQUIRESUPDATE:False,}, {NAME:self.hostName,})
                connection.close()
                
            # If not provided we take the current list of pathsToCheck from the database
            if usedb:
                connection = Connector(dbname, dbuser, dbhost).getConnection()
                pathsToCheckLEDDB = LEDDBOps.getTableColValues(connection, PARENTPATH)
                connection.close()
                for i in range(len(pathsToCheckLEDDB)):
                    pathsToCheckLEDDB[i] = msoperations.getParentPath(pathsToCheckLEDDB[i])
                pathsToCheck =  list(set(pathsToCheckLEDDB))
            else:
                pathsToCheck =  path.split(',')
            
            ldsPaths = []
            for pathToCheck in pathsToCheck:
                ldsPaths.extend(nodeoperations.getLDSPaths(pathToCheck))
    
            removedCounter = 0
            totalMSPs = 0
            errorMSPs = 0
            newAddedMSPs = 0
            updatedGains = 0
            updatedQuality = 0
            if not len(ldsPaths):
                print 'None LDS paths found with current configuration'    
            else:
                parents = []
                for i in range(len(ldsPaths)):
                    parents.append('parent') # We will execute a single parent         
                
                # Distribute the tasks, only one parent, it will create maxPaths kids, 
                # each of them in charge of processing the MSPs found in related LDS path
                (retValuesOk, retValuesKo) = td.distribute(parents, ldsPaths, self.function, numprocessors, 1, dynamicLog = False)
                for (errorCounter, totalCounter, newMSPsCounter, updatedGainsCounter, updatedQualityCounter) in retValuesOk:
                    errorMSPs += errorCounter
                    totalMSPs += totalCounter
                    newAddedMSPs += newMSPsCounter
                    updatedGains += updatedGainsCounter
                    updatedQuality += updatedQualityCounter
                td.showKoAll(retValuesKo)
                if not notclean:
                    print 'Cleaning old references'
                    connection = Connector(dbname, dbuser, dbhost).getConnection()
                    diagUpdater = DiagnosticUpdater(connection)
                    rows = LEDDBOps.select(connection, MSP, {(LASTCHECK):(currentTimeStamp,'<'),(HOST):self.hostName}, columnNames = [MS+ID,MSP+ID,PARENTPATH,NAME])
                    for row in rows:
                        msppath = row[2] + '/' + row[3]
                        if not os.path.isdir(msppath) and not os.path.isfile(msppath):
                            #It does not exist anymore, we can delete thir row from the DB
                            removedCounter += 1
                            print 'Removing ' + MSP + ' row ' + self.hostName + ': ' + msppath
                            LEDDBOps.delete(connection, MSP, {MSP+ID:row[1],})
                            diagUpdater.updateMeta(row[0], False)
                    connection.close()
            
            if totalMSPs:
                stats = 'STATS - Total processed MSPs: ' + str(totalMSPs) + '. Added: ' + str(newAddedMSPs) + '. Errors: ' + str(errorMSPs) + ' (' + ('%.2f' % (errorMSPs * 100./totalMSPs)) + '%)' + '. Updated Gains: ' + str(updatedGains) + ' (' + ('%.2f' % (updatedGains * 100./totalMSPs)) + '%). ' + 'Updated Quality: ' + str(updatedQuality) + ' (' + ('%.2f' % (updatedQuality * 100./totalMSPs)) + '%). ' + 'Deleted: ' + str(removedCounter)
            else:
                stats = 'STATS - Total processed MSPs: ' + str(totalMSPs) + '. Added: ' + str(newAddedMSPs) + '. Errors: ' + str(errorMSPs) + ' (' + ('%.2f' % (0)) + '%)' + '. Updated Gains: ' + str(updatedGains) + ' (' + ('%.2f' % (0)) + '%). ' + 'Updated Quality: ' + str(updatedQuality) + ' (' + ('%.2f' % (0)) + '%). ' + 'Deleted: ' + str(removedCounter)
            print utils.getCurrentTimeStamp() + ' - finished LEDDB node updater in ' + self.hostName + ' ( ' + str(int(time.time()-tstart)) + ' seconds). ' + stats