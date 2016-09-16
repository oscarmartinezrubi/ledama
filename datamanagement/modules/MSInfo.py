################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import diagoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile
from ledama.MSP import MSP

class MSInfo(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('inputarg', 'i', 'Input RefFile or MS path')
        # the information
        information = """Shows information about the contents of a refFile or of a single MS. If a refFile is provided we assume all MSs have same averaging properties."""
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def getSize(self, valueInMB):
        if valueInMB > 1048576:
            return (('%.2f' % (float(valueInMB) / float(1048576))),'TB')
        elif valueInMB > 1024:
            return (('%.2f' % (float(valueInMB) / float(1024))),'GB')
        else:
            return (('%.2f' % float(valueInMB)), 'MB')
    
    def process(self, inputarg):
        refFile = None
        msAbsPath = None
        host = utils.getHostName()
        size = None
        if os.path.isfile(inputarg):
            refFile = ReferenceFile(inputarg)
            msAbsPath = '/net/' + refFile.nodes[0] + refFile.absPaths[0]
            if os.path.islink(msAbsPath):
                msAbsPath = '/net/' + refFile.nodes[0] + os.readlink(msAbsPath)
            size = refFile.sizes[0]
            
            beamDict = {}
            
            for i in range(len(refFile.absPaths)):
                beamIndex = refFile.beamIndexes[i]
                if beamIndex not in beamDict:
                    beamDict[beamIndex] = []
                p = '/net/' + refFile.nodes[i] + refFile.absPaths[i]
                if os.path.islink(p):
                    p = '/net/' + refFile.nodes[i] + os.readlink(p)
                beamDict[beamIndex].append((p, refFile.sizes[i]))
                
            print '# Number of MSPs: ' + str(len(refFile.absPaths))
            try:
                (sizeStr, sizeUnitsStr) = self.getSize(sum(refFile.sizes))
                print '# Total size: ' + str(sizeStr) + ' ' + sizeUnitsStr
            except:
                print '# Total size: ?'
            print '# Frequency range (MHz): ' + refFile.getFreqRange()
            print '# Number of Beams: ' + str(len(beamDict))
            for beamIndex in beamDict:
                print '#    Beam ' + str(beamIndex) + ' - Number of MSPs: ' + str(len(beamDict[beamIndex]))
                (bAbsPath, bSize) = beamDict[beamIndex][0]
                msp = MSP(bAbsPath, host, bSize)
                msp.loadReferencingData()
                print '#    Beam ' + str(beamIndex) + ' - Field: ' + str(msp.field)
                print '#    Beam ' + str(beamIndex) + ' - Phase direction  RA: ' + str(msp.phaseDirRA)
                print '#    Beam ' + str(beamIndex) + ' - Phase direction DEC: ' + str(msp.phaseDirDec) + "\n"
                
            print "# Next information based on: " + refFile.nodes[0] + ' ' + refFile.absPaths[0]
        else:
            msAbsPath = inputarg
            
        if not os.path.isdir(msAbsPath):
            print msAbsPath + ' is not found'
            return
        
        if size == None:
            size = msoperations.getSize(msAbsPath)
        msp = MSP(msAbsPath, host, size)
        msp.loadReferencingData()
            
        print '#    LDS: ' + str(msp.lds)
        print '#    Initial UTC: ' + str(msp.initialUTC) 
        print '#    Final UTC  : ' + str(msp.finalUTC)
        print '#    Project: ' + str(msp.project)
        print '#    Antenna Type: ' + str(msp.antennaType)
        numStations = len(msp.stations)
        print '#    Num. Stations: ' + str(numStations)
        print '#    Num. Baselines: ' + str(int(numStations * (numStations-1)  / 2.))
        print '#    Stations: ' + str(msp.stations)
        
        if refFile == None:
            print '#    Beam : ' + str(msp.beamIndex)
            print '#    Field: ' + str(msp.field)
            print '#    Phase direction  RA: ' + str(msp.phaseDirRA)
            print '#    Phase direction DEC: ' + str(msp.phaseDirDec)
            
        print '#    Version Index: ' + str(msp.versionIndex)
        print '#    Applications: ' + ', '.join(msp.applications)
        print '#    Is Flagged: ' + str(msp.flagged)
        print '#    Is Averaged: ' + str(msp.averaged)
        print '#    Is Calibrated: ' + str(msp.calibrated)
        print '#    Has CORRECTED_DATA: ' + str(msp.hasCorrectedData)
        print '#    Has MODEL_DATA: ' + str(msp.hasModelData)
        print '#    Has Gains: ' + str(diagoperations.hasGain(msAbsPath))
        print '#    Has Quality: ' + str(diagoperations.hasQuality(msAbsPath))
        print '#    Integration Time: ' + str(msp.intTime) + ' s'
        print '#    Num. Channels: ' + str(msp.numChan)
        print '#    Total Bandwidth: ' + str(msp.totalBandwidth) + ' MHz'
        try:
            (sizeStr, sizeUnitsStr) = self.getSize(msp.size)
            print '#    Size: ' + str(sizeStr) + ' ' + sizeUnitsStr + "\n"
        except:
            print '#    Size: ?\n'
        
