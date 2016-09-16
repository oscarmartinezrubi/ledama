#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import bisect
from ledama.leddb.Naming import *
import ledama.diagoperations as do
import ledama.config as lconfig
import ledama.leddb.LEDDBOps as LEDDBOps

class DiagnosticUpdater:
    """Class to update a diagnostic parameters related to a MS in the LEDDB"""
    def __init__(self, connection):
        # Get a connection to the LEDDB
        self.connection = connection
        self.debug = lconfig.DEBUG
        (self.partitionsSuffix, self.nodePoints) = do.getPartitionSchema(self.connection)
        
    # Commit the information of this MS to the LEDDB (return True if new MSP has been added)
    def updateGain(self, msp):
        # First we need to check if there is gain data for this MS
        timeStamp = do.getTimeStamp(msp.absPath, do.GAIN_TAB_NAME)
        gainPartition = do.getPartition(GAIN, self.partitionsSuffix, self.nodePoints, msp.msId)
        
        if timeStamp != None:
            # If it is different than None it means there is some sort of GAIN data
            # Get the oldest GAIN in the LEDDB related to the current MSP 
            minLastCheck = LEDDBOps.select(self.connection, gainPartition, {MS+ID:msp.msId,}, ['min(' + LASTCHECK + ')',], toPrint = self.debug)[0][0]
            if minLastCheck != None: 
                if timeStamp > minLastCheck:
                    # timeStamp is newer than oldest GAIN in the LEDDB -> we need to update
                    # We get the new data
                    gain = do.getDiagData(msp.absPath, do.GAIN_TAB_NAME) 
                    # we only delete old one if new one is correctly loaded
                    if gain != None:
                        print 'Removing old GAIN diagnostic for ' + msp.absPath
                        LEDDBOps.delete(self.connection, gainPartition, {MS+ID:msp.msId,})
                else:
                    # The data is already updated
                    return (True, False)
            else:
                # There is no data related to this MSP yet, let's load it
                gain = do.getDiagData(msp.absPath, do.GAIN_TAB_NAME)
            
            if gain == None:
                return (False, False)
            
            print 'Adding GAIN diagnostic for ' + msp.absPath
            
            (freqStep, timeStep, data) = gain
            
            for (station, direction) in data:
                (directionRA,directionD) = direction
                dataUpdating = {MS+ID:msp.msId,
                               STATION:station,
                               DIRRA:directionRA,
                               DIRDEC:directionD,
                               FSTEP:freqStep,
                               TSTEP:timeStep, 
                               LASTCHECK:timeStamp, 
                               VALUES:data[(station,direction)]}
                LEDDBOps.insert(self.connection, gainPartition, LEDDBOps.removeNoneValues(dataUpdating))
            return (True,True)
        else:
            rows = LEDDBOps.select(self.connection, MSMETA, {MSMETA+ID:msp.msId,}, [MSHASGAIN],)
            if len(rows):
                hadGain = LEDDBOps.getColValue(rows[0])
                if hadGain:
                    print 'Removing old GAIN diagnostic for ' + msp.absPath
                    LEDDBOps.delete(self.connection, gainPartition, {MS+ID:msp.msId,})
                    return (False, True)
            return (False, False)
            
    def updateQuality(self, msp):
        timeStamp = do.getTimeStamp(msp.absPath, do.QTS_TAB_NAME)
        
        qtsPartition = do.getPartition(QTSTAT, self.partitionsSuffix, self.nodePoints, msp.msId)
        qfsPartition = do.getPartition(QFSTAT, self.partitionsSuffix, self.nodePoints, msp.msId)
        qbsPartition = do.getPartition(QBSTAT, self.partitionsSuffix, self.nodePoints, msp.msId)
        
        if timeStamp != None:
            if msp.versionIndex > 1:
                print 'Ignoring quality diagnostic for ' + msp.absPath
                return (False, False)
            minLastCheck = LEDDBOps.select(self.connection, qtsPartition, {MS+ID:msp.msId,}, ['min(' + LASTCHECK + ')',], toPrint = self.debug)[0][0]
            if minLastCheck != None: 
                if timeStamp > minLastCheck:
                    qts = do.getDiagData(msp.absPath, do.QTS_TAB_NAME) 
                    if qts != None:
                        print 'Removing old QTS diagnostic for ' + msp.absPath
                        LEDDBOps.delete(self.connection, qtsPartition, {MS+ID:msp.msId,})
                    qfs = do.getDiagData(msp.absPath, do.QFS_TAB_NAME)
                    if qfs != None:
                        print 'Removing old QFS diagnostic for ' + msp.absPath
                        LEDDBOps.delete(self.connection, qfsPartition, {MS+ID:msp.msId,})
                    qbs = do.getDiagData(msp.absPath, do.QBS_TAB_NAME)
                    if qbs != None:
                        print 'Removing old QBS diagnostic for ' + msp.absPath
                        LEDDBOps.delete(self.connection, qbsPartition, {MS+ID:msp.msId,})
                else:
                    # The data is already updated
                    return (True, False)
            else:
                # There is no data related to this MSP yet, let's load it
                qts = do.getDiagData(msp.absPath, do.QTS_TAB_NAME)
                qfs = do.getDiagData(msp.absPath, do.QFS_TAB_NAME)
                qbs = do.getDiagData(msp.absPath, do.QBS_TAB_NAME)
                
            if qts == None or qfs == None or qbs == None:
                return (False, False)
            else:
                print 'Adding QTS diagnostic for ' + msp.absPath
                (freqStep, timeStep, qKNames, data) = qts
                for i in range(len(qKNames)):
                    qKId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(self.connection, QKIND, {NAME:qKNames[i]}, None, [QKIND+ID,]))
                    dataUpdating = {MS+ID:msp.msId,
                                   QKIND + ID:qKId,
                                   FSTEP:freqStep,
                                   TSTEP:timeStep, 
                                   LASTCHECK:timeStamp, 
                                   VALUES:data[i]}
                    LEDDBOps.insert(self.connection, qtsPartition, LEDDBOps.removeNoneValues(dataUpdating))
                
                print 'Adding QFS diagnostic for ' + msp.absPath
                (freqStep, qKNames, data) = qfs
                for i in range(len(qKNames)):
                    qKId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(self.connection, QKIND, {NAME:qKNames[i]}, None, [QKIND+ID,]))
                    dataUpdating = {MS+ID:msp.msId,
                                   QKIND + ID:qKId,
                                   FSTEP:freqStep,
                                   LASTCHECK:timeStamp, 
                                   VALUES:data[i]}
                    LEDDBOps.insert(self.connection, qfsPartition, LEDDBOps.removeNoneValues(dataUpdating))
                 
                print 'Adding QBS diagnostic for ' + msp.absPath
                (freqStep, qKNames, baselines, data) = qbs
                for i in range(len(qKNames)):
                    qKId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(self.connection, QKIND, {NAME:qKNames[i]}, None, [QKIND+ID,]))
                    qKData = data[i]
                    for j in range(len(baselines)):
                        (ant1,ant2) = baselines[j]
                        dataUpdating = {MS+ID:msp.msId,
                                       QKIND + ID:qKId,
                                       BASELINE+ID:LEDDBOps.getColValue(LEDDBOps.select(self.connection,BASELINE, {STATION1:ant1,STATION2:ant2}, [BASELINE+ID])[0]),
                                       FSTEP:freqStep,
                                       LASTCHECK:timeStamp, 
                                       VALUES:qKData[j]}
                        LEDDBOps.insert(self.connection, qbsPartition, LEDDBOps.removeNoneValues(dataUpdating))
                return (True,True)
        else:
            rows = LEDDBOps.select(self.connection, MSMETA, {MSMETA+ID:msp.msId,}, [MSHASQUALITY],)
            if len(rows):
                hadQuality = LEDDBOps.getColValue([0])
                if hadQuality:
                    # We remove all the quality since they will all come together
                    print 'Removing old QTS diagnostic for ' + msp.absPath
                    LEDDBOps.delete(self.connection, qtsPartition, {MS+ID:msp.msId,})
                    print 'Removing old QFS diagnostic for ' + msp.absPath
                    LEDDBOps.delete(self.connection, qfsPartition, {MS+ID:msp.msId,})
                    print 'Removing old QBS diagnostic for ' + msp.absPath
                    LEDDBOps.delete(self.connection, qbsPartition, {MS+ID:msp.msId,})
                    return (False, True)
            return (False, False)
       
    def updateMeta(self, msId, hasMSP = None, hasGain = None, hasQuality = None, hasGainMovie = None):
        LEDDBOps.updateUniqueRow(self.connection, MSMETA, {MSMETA+ID:msId,}, {MSHASMSP:hasMSP,MSHASGAIN:hasGain,MSHASQUALITY: hasQuality, MSHASGAINMOVIE: hasGainMovie})