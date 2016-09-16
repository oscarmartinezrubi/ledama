#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import *
import ledama.utils as utils
import ledama.config as lconfig
import ledama.leddb.LEDDBOps as LEDDBOps
import ledama.msoperations as msoperations
import itertools

class MSPUpdater:
    """Class to update a MeasurementSetProduct to the LEDDB"""
    def __init__(self, connection):
        # Get a connection to the LEDDB
        self.connection = connection
        self.debug = lconfig.DEBUG

    # Commit the information of this MS to the LEDDB (return True if new MSP has been added)
    def process(self, msp):
        # We search if there is some MSP in this location/name/node
        mspRow = LEDDBOps.updateUniqueRow(self.connection,[MS,MSP,LDSBP],
                                       {MSP+'.'+MS+ID:MS+'.'+MS+ID,MS+'.'+LDSBP+ID:LDSBP+'.'+LDSBP+ID, NAME:msp.name, PARENTPATH:msp.parentPath, HOST:msp.host}, 
                                       None, 
                                       [MSP+'.'+MS+ID, MS+'.'+LDSBP+ID, LDSBP+'.'+LDSB+ID, SIZE, MS+'.'+CENTFREQ,MS+'.'+BW], 
                                       False, toPrint = self.debug)
        if mspRow == None:
            print 'Adding ' + msp.host + ' ' + msp.absPath
            self.commit(msp)
            return True
        else:
            print 'Updating ' + msp.host + ' ' +msp.absPath
            msp.setIds(LEDDBOps.getColValue(mspRow,0), LEDDBOps.getColValue(mspRow,1), LEDDBOps.getColValue(mspRow,2))
            self.update(msp, LEDDBOps.getColValue(mspRow,3), LEDDBOps.getColValue(mspRow,4), LEDDBOps.getColValue(mspRow,5))
            return False
        
    # Commit the information of this MS to the LEDDB
    def commit(self, msp):

        #Update project, field, beam, antType and epoch tables
        LEDDBOps.updateUniqueRow(self.connection, PROJECT, {NAME:msp.project,})
        LEDDBOps.updateUniqueRow(self.connection, ANTTYPE, {NAME:msp.antennaType,})
          
        # Update data in LDS row if new data is available
        dataForLDSSelection = {}
        dataForLDSSelection[NAME] = msp.lds
        dataForLDSUpdating = {}
        dataForLDSUpdating[PROJECT] = msp.project
        dataForLDSUpdating[ANTTYPE] = msp.antennaType
        dataForLDSUpdating[INITIALUTC] = msp.initialUTC
        dataForLDSUpdating[FINALUTC] = msp.finalUTC
        dataForLDSUpdating[INITIALMJD] = msp.initialMJD
        dataForLDSUpdating[FINALMJD] = msp.finalMJD
        LEDDBOps.updateUniqueRow(self.connection, LDS, dataForLDSSelection, dataForLDSUpdating, updateOnlyIfColumnMissing = True, toPrint = self.debug)
        
        # Add information related to the stations
        if msp.stations != None:
            # This is a bit expensive so, if somebody has started this op, we do not do it again
            numCurrentStations = len(LEDDBOps.select(self.connection, LDSHASSTATION, {LDS:msp.lds}, [STATION]))
            if numCurrentStations == 0:
                for i in range(len(msp.stations)):
                    station = msp.stations[i]
                    LEDDBOps.updateUniqueRow(self.connection, STATION, {NAME:station},{ANTTYPE:msp.antennaType,LOCATIONTYPE:msoperations.getStationLocationType(station), POSITION:msp.stationsPositions[i].tolist()}, updateOnlyIfRowMissing = True)
                    LEDDBOps.updateUniqueRow(self.connection, LDSHASSTATION, {LDS:msp.lds, STATION:station})
                
                baselines = itertools.combinations_with_replacement(sorted(msp.stations),2)
                for (ant1,ant2) in baselines:
                    LEDDBOps.updateUniqueRow(self.connection, BASELINE, {STATION1:ant1,STATION2:ant2})
                    
        # Update the field required for the LDSB
        LEDDBOps.updateUniqueRow(self.connection, FIELD, {NAME:msp.field,})
        
        # Update data in LDSB row if new data is available
        dataForLDSBSelection = {}
        dataForLDSBSelection[LDS] = msp.lds
        dataForLDSBSelection[BEAMINDEX] = msp.beamIndex
        dataForLDSBUpdating = {}
        dataForLDSBUpdating[FIELD] = msp.field
        dataForLDSBUpdating[PHASEDIRRA] = msp.phaseDirRA
        dataForLDSBUpdating[PHASEDIRDEC] = msp.phaseDirDec
        lofarDataSetBeamId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(self.connection, LDSB, dataForLDSBSelection, dataForLDSBUpdating, [LDSB+ID,], updateOnlyIfColumnMissing = True, toPrint = self.debug))
        
        LEDDBOps.updateUniqueRow(self.connection, STORE, {NAME:msp.store,})
        
        #Get the ID of the related LSDP
        dataForLDSBPSelection = {}
        dataForLDSBPSelection[LDSB+ID] = lofarDataSetBeamId
        dataForLDSBPSelection[STORE] = msp.store
        dataForLDSBPSelection[INTTIME] = msp.intTime
        dataForLDSBPSelection[NUMCHAN] = msp.numChan
        dataForLDSBPSelection[VERSION] = msp.versionIndex
        dataForLDSBPSelection[RAW] = msp.raw
        dataForLDSBPSelection[TAR] = msp.tar
        dataForLDSBPSelection[BVF] = msp.bvf
        
        dataForLDSBPUpdating = {}
        dataForLDSBPUpdating[ADDDATE] = utils.getCurrentUTCTime()
        dataForLDSBPUpdating[DESCR] = msp.ldsbpDescr
        dataForLDSBPUpdating[FLAGGED] = msp.flagged
        dataForLDSBPUpdating[AVERAGED] = msp.averaged
        dataForLDSBPUpdating[CALIBRATED] = msp.calibrated
        dataForLDSBPUpdating[DIRDEPCAL] = msp.ddCal
        dataForLDSBPUpdating[DIRINDEPCAL] = msp.diCal
        dataForLDSBPUpdating[MAINUSER] = msp.mainUser
        
        lofarDataSetBeamProductId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(self.connection, LDSBP, dataForLDSBPSelection, dataForLDSBPUpdating, [LDSBP+ID,], updateOnlyIfRowMissing = True, updateOnlyIfColumnMissing = True, toPrint = self.debug)) 

        # Get the ID of the related MS
        dataForMSSelection = {}
        dataForMSSelection[LDSBP + ID] = lofarDataSetBeamProductId
        dataForMSSelection[SBINDEX] = msp.sbIndex
        dataForMSUpdating = {}
        dataForMSUpdating[CENTFREQ] = msp.centralFrequency
        dataForMSUpdating[BW] = msp.totalBandwidth
                
        # We try to get the MSI Id from the data
        measurementSetInformationId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(self.connection, MS, dataForMSSelection, dataForMSUpdating, [MS+ID,], updateOnlyIfColumnMissing = True, toPrint = self.debug))
        
        # Update Host and Location
        LEDDBOps.updateUniqueRow(self.connection, PARENTPATH, {NAME:msp.parentPath,})
        LEDDBOps.updateUniqueRow(self.connection, HOST, {NAME:msp.host,})
        
        #Get the ID of the related MSPR
        dataForMSPSelection = {}
        dataForMSPSelection[MS+ID] = measurementSetInformationId
        dataForMSPSelection[HOST] = msp.host
        dataForMSPSelection[PARENTPATH] = msp.parentPath
        dataForMSPSelection[NAME] = msp.name
        dataForMSPUpdating = {}
        dataForMSPUpdating[SIZE] = msp.size
        dataForMSPUpdating[LASTCHECK] = utils.getCurrentTime()
        dataForMSPUpdating[LASTMODIFICATION] = utils.getCurrentTime(utils.getLastModification(msp.absPath))
        
        # We try to get the MSI Id from the data
        LEDDBOps.updateUniqueRow(self.connection, MSP, dataForMSPSelection, dataForMSPUpdating, updateOnlyIfColumnMissing = True, toPrint = self.debug)
        
        msp.setIds(measurementSetInformationId, lofarDataSetBeamProductId, lofarDataSetBeamId)
        
    # Commit the information of this MS to the LEDDB
    def update(self, msp, sizeLEDDB, centFreqLEDDB, bwLEDDB):
        
        #There are several things to be checked.
        # - The LDSBP has changed
        # - The LDS information is not updated
        # - The LDSB information is not updated
        # - The file may have the same size or not.
        #In all cases we must update the lastCheck and the last modification
        
        # Get the expected LDSP Id with current data
        dataForLDSBPSelection = {}
        dataForLDSBPSelection[LDSB+ID] = msp.ldsbId
        dataForLDSBPSelection[STORE] = msp.store
        dataForLDSBPSelection[INTTIME] = msp.intTime
        dataForLDSBPSelection[NUMCHAN] = msp.numChan
        dataForLDSBPSelection[VERSION] = msp.versionIndex
        dataForLDSBPSelection[RAW] = msp.raw
        dataForLDSBPSelection[TAR] = msp.tar
        dataForLDSBPSelection[BVF] = msp.bvf
        ldsbpIdLEDDB = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(self.connection, LDSBP, dataForLDSBPSelection, None, [LDSBP+ID,], False, toPrint = self.debug)) 
        
        if ldsbpIdLEDDB != msp.ldsbpId:
            # If the LDSBP has changed, we delete the last row and add a new one
            LEDDBOps.delete(self.connection, MSP, {NAME:msp.name,PARENTPATH:msp.parentPath,HOST:msp.host})
            self.commit(msp)
        else:
            
            # This code was only necessary when the data had no all the fields from the beginning, now they do so 
            # we can comment this. 
            # We update (probably) the epoch information and the antennas type
            #LEDDBOps.updateUniqueRow(self.connection, LDS, {NAME:msp.lds}, {INITIALUTC:msp.initialUTC,FINALUTC:msp.finalUTC,INITIALMJD:msp.initialMJD,FINALMJD:msp.finalMJD,ANTTYPE:msp.antennaType,PROJECT:msp.project})
            # We update (probably) the field and phase dir information
            #LEDDBOps.updateUniqueRow(self.connection, LDSB, {LDSB+'.'+LDSB+ID:msp.ldsbId}, {FIELD:msp.field,PHASEDIRRA:msp.phaseDirRA,PHASEDIRDEC:msp.phaseDirDec})
                        
            if centFreqLEDDB == None and msp.centralFrequency != None:
                print 'Updating (' + CENTFREQ + ', ' + BW  +  ') from (' + str(centFreqLEDDB) + ', ' + str(bwLEDDB) + ') to ('+ str(msp.centralFrequency) + ', ' + str(msp.totalBandwidth) + ') in ' +  msp.host + ': ' + msp.absPath
                LEDDBOps.update(self.connection, MS, {CENTFREQ:msp.centralFrequency, BW: msp.totalBandwidth}, {MS+'.'+MS+ID: msp.msId})
                        
            # Now let's check size
            if msp.size != sizeLEDDB:
                print 'Updating ' + (SIZE) + ' from ' + str(sizeLEDDB) + ' to '+ str(msp.size) + ' in ' +  msp.host + ': ' + msp.absPath
                LEDDBOps.update(self.connection, MSP, {SIZE:msp.size}, {NAME:msp.name,PARENTPATH:msp.parentPath,HOST:msp.host,SIZE:sizeLEDDB})
    
            # Let's update lastCheck and alstMod
            LEDDBOps.update(self.connection, MSP, 
                            {LASTCHECK:utils.getCurrentTime(),LASTMODIFICATION:utils.getCurrentTime(utils.getLastModification(msp.absPath))}, 
                            {NAME:msp.name,PARENTPATH:msp.parentPath,HOST:msp.host})