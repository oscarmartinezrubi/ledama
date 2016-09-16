#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import msoperations
from ledama import utils
from ledama.leddb import LEDDBOps
from ledama.leddb.Naming import *
from ledama.leddb.Connector import *

class MSP:
    """Measurement Set Product attributes that are frequently asked"""

    # Initialize the MSP all None
    def __init__(self, absPath, host, size):
        
        self.absPath = absPath
        
        self.host = host
        self.size = size
        self.name = msoperations.getMeasurementSetName(self.absPath)
        self.parentPath = msoperations.getParentPath(self.absPath)
        
        self.lds = msoperations.getLDSName(self.absPath)
        self.sbIndex = msoperations.getSBIndex(self.absPath)
        self.raw = msoperations.isRaw(self.absPath)
        self.tar = msoperations.isTar(self.absPath)
        self.bvf = msoperations.isBvf(self.absPath)
        self.beamIndex = msoperations.getBeamIndex(self.absPath)
        self.versionIndex = msoperations.getVersionIndex(self.absPath)
        self.store = msoperations.getStore(self.host, self.absPath)
        self.intTime = -1.  
        self.numChan = -1
        
        # These are the rest of attributes that are not by default loaded
        # REFERENCING
        self.project = None
        self.field = None
        self.initialUTC = None
        self.finalUTC = None
        self.initialMJD = None
        self.finalMJD = None
        self.antennaType = None
        self.stations = None
        self.stationsPositions = None
        
        self.ldsbpDescr = None
        self.mainUser = None
        self.phaseDirRA = None
        self.phaseDirDec = None
        self.applications = None
        self.flagged = None
        self.averaged = None
        self.calibrated = None
        self.hasCorrectedData = None
        self.hasModelData = None
        self.ddCal = None
        self.diCal = None

        self.centralFrequency = None
        self.totalBandwidth = None
        
        # IDs
        self.msId = None
        self.ldsbId = None
        self.ldsbpId = None
        
    # Load all the variables related to the referencing 
    def loadReferencingData(self):
        try:
            self.mainUser = utils.getOwner(self.absPath)
        except:
            self.mainUser = None
        
        if self.tar:
                self.intTime = -1
                self.numChan = -1
                self.ldsbpDescr = 'TAR'
        elif self.bvf:
                self.intTime = -2
                self.numChan = -2 
                self.ldsbpDescr = 'BVF'
        else:
            if self.host != utils.getHostName():
                # We are processing a MSP which is not available
                self.intTime = -3
                self.numChan = -3 
                self.ldsbpDescr = 'UNKNOWN'
            else:
                # The MSP is supposed to be available
                try:
                    # We read the main table just once
                    msTable = msoperations.getTable(self.absPath) 
                    self.project = msoperations.getProject(msTable, self.absPath)
                    self.field = msoperations.getField(msTable, self.absPath)
                    (self.initialUTC, self.finalUTC) = msoperations.getEpochUTC(msTable, self.absPath)
                    (self.initialMJD, self.finalMJD) = msoperations.getEpochMJD(msTable, self.absPath)
                    self.antennaType = msoperations.getAntennaType(msTable)
                    
                    self.stations = msoperations.getStations(msTable)
                    self.stationsPositions = msoperations.getStationsPositions(msTable)
                    
                    self.intTime = msoperations.getIntegrationTime(msTable)
                    self.numChan = msoperations.getNumberOfChannels(msTable)
                    
                    if self.intTime < 0 or self.numChan < 0:
                        self.intTime = -4
                        self.numChan = -4 
                        self.ldsbpDescr = 'ERROR'
                    else:
                        self.ldsbpDescr =  str(self.numChan) + ' ' + str(int(round(self.intTime))) 
                    
                    (self.phaseDirRA, self.phaseDirDec) = msoperations.getPhaseDirection(msTable)
                    self.applications = msoperations.getApplications(msTable)
                    (flagApps,averApps,calApps) = msoperations.getPipelineDescr(self.applications)
                    if len(flagApps) or len(averApps):
                        self.flagged = True
                    else:
                        self.flagged = False
                    
                    if len(averApps):
                        self.averaged = True
                    else:
                        self.averaged = False
                    if len(calApps):
                        self.calibrated = True
                    else:
                        self.calibrated = False
                    self.ddCal = False
                    self.diCal = False
        
                    self.centralFrequency = msoperations.getCentralFrequency(msTable)
                    self.totalBandwidth = msoperations.getTotalBandwith(msTable)
                    
                    colnames = msTable.colnames()
                    self.hasCorrectedData = False
                    self.hasModelData = False
                    if 'CORRECTED_DATA' in colnames:
                        self.hasCorrectedData = True
                        self.calibrated = True
                    if 'MODEL_DATA' in colnames:
                        self.hasModelData = True
                except:
                    # It had some error that made the MSP unavailable
                    self.intTime = -4
                    self.numChan = -4 
                    self.ldsbpDescr = 'ERROR'

# Load all the variables related to the referencing 
    def loadReferencingDataAkin(self, connection):
        # The MSP is not available, but we load its info from LEDDB same version
        self.intTime = -3
        self.numChan = -3 
        self.ldsbpDescr = 'UNKNOWN'
        
        ldsrows = LEDDBOps.select(connection, LDS, {NAME:self.lds}, columnNames = [PROJECT, ANTTYPE, INITIALMJD, FINALMJD, INITIALUTC, FINALUTC])
        
        if len(ldsrows):
            (self.project, self.antennaType, self.initialMJD, self.finalMJD, self.initialUTC, self.finalUTC) = ldsrows[0]
        
            stationrows = LEDDBOps.select(connection, LDSHASSTATION, {LDS:self.lds}, columnNames = [STATION,])
            
            if len(stationrows):
                self.stations = []
                self.stationsPositions = []
                for stationrow in stationrows:
                    station = stationrow[0]
                    self.stations.append(station)
                    positionrows = LEDDBOps.select(connection, STATION, {NAME:station}, columnNames = [POSITION,])
                    self.stationsPositions.append(positionrows[0][0])
            
            ldsbrows = LEDDBOps.select(connection, LDSB, {LDS:self.lds, BEAMINDEX: self.beamIndex}, columnNames = [LDSB+ID, FIELD, PHASEDIRRA, PHASEDIRDEC])
            
            if len(ldsbrows):
                (self.ldsbId, self.field, self.phaseDirRA, self.phaseDirDec) = ldsbrows[0]
                
                ldsbprows = LEDDBOps.select(connection, LDSBP, {LDSB+ID:self.ldsbId, VERSION: self.versionIndex, STORE: utils.EOR, INTTIME: (0, '>')}, columnNames = [LDSBP+ID, INTTIME, NUMCHAN, MAINUSER, DESCR,FLAGGED,AVERAGED ,CALIBRATED,DIRDEPCAL,DIRINDEPCAL ])
                
                if len(ldsbprows):
                    (ldsbpId, self.intTime,self.numChan,self.mainUser, self.ldsbpDescr,self.flagged,self.averaged,self.calibrated,self.ddCal,self.diCal) = ldsbprows[0]
                    self.hasCorrectedData = False
                    self.hasModelData = False
                    
                    msrows = LEDDBOps.select(connection, MS, {LDSBP+ID:ldsbpId, SBINDEX: self.sbIndex}, columnNames = [CENTFREQ, BW])
                    
                    if len(msrows):
                        (self.centralFrequency,self.totalBandwidth) = msrows[0]
    
    # Set the IDs
    def setIds(self, msId, ldsbpId, ldsbId):
        self.msId = msId
        self.ldsbpId = ldsbpId
        self.ldsbId = ldsbId
        