#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, bisect
import numpy
import ledama.utils as utils
import pyrap.tables as pt
from ledama import msoperations
from ledama.leddb.modules.CreateLEDDBPartitions import CreateLEDDBPartitions
from ledama.leddb.Naming import GAIN
    
try:
    import lofar.parmdb as pdb
except ImportError:
    print "Error: The lofar paramdb module is not available."

PARAM_NAME_PATTERN = '%s:%s:%s:%s'
AMPL_COORD = 'Ampl'
PHASE_COORD = 'Phase'
REAL_COORD = 'Real'
IMAG_COORD = 'Imag'
GAIN_TYPE = 'Gain'
CORRS_NAMES = ['0:0', '0:1', '1:0', '1:1']

# Table names related to each one of the diagnostic
GAIN_TAB_NAME = 'instrument'
QTS_TAB_NAME = 'QUALITY_TIME_STATISTIC'
QFS_TAB_NAME = 'QUALITY_FREQUENCY_STATISTIC'
QBS_TAB_NAME = 'QUALITY_BASELINE_STATISTIC'

# Get the time stamp of a tabName (this table contains diagnostic data) or None
# if the tab is not found
def getTimeStamp(absPath, tabName):
    # We get all the dates of all the files within tabName folder
    # we return the bigger one
    t = utils.getLastModification(absPath + '/' + tabName)
    if t != None:
        return utils.getCurrentTime(t)
    return None

# Get diagnostic data for the MS in abspath for the diag type indicated by tabName
def getDiagData(absPath, tabName):
    if tabName == GAIN_TAB_NAME:
        return getGain(absPath)
    elif tabName == QTS_TAB_NAME:
        return getQTS(absPath)
    elif tabName == QFS_TAB_NAME:
        return getQFS(absPath)
    elif tabName == QBS_TAB_NAME:
        return getQBS(absPath)
    raise Exception('Unexpected diagnostic type: ' + tabName)
#
# GAIN CODE
#
# This method gets the (real, imag) of a paramdb for a specified station, corr, polar
def getComplexCorrelationValues(pdata, station, corr , polar, cords):
    if polar:
        valsamp = pdata[PARAM_NAME_PATTERN % (GAIN_TYPE,corr,AMPL_COORD,station)]['values']
        valsphase = pdata[PARAM_NAME_PATTERN % (GAIN_TYPE,corr,PHASE_COORD,station)]['values']
        valsreal = valsamp * numpy.cos(valsphase)
        valsimag = valsamp * numpy.sin(valsphase)
    else:
        if REAL_COORD in cords:
            valsreal =pdata[PARAM_NAME_PATTERN%(GAIN_TYPE,corr,REAL_COORD,station)]['values']
        if IMAG_COORD in cords:
            valsimag = pdata[PARAM_NAME_PATTERN%(GAIN_TYPE,corr,IMAG_COORD,station)]['values']
        # Checking if null entries for some of the coordinates
        # we use the other one to create a zeros array
        if valsreal is None:
            valsreal = numpy.zeros(valsimag.shape)
        elif valsimag is None:
            valsimag = numpy.zeros(valsreal.shape)
    # In this point the dimensions are complex,time,freq, we convert it to freq,time,complex
    return numpy.array([valsreal,valsimag]).transpose((2,1,0))

# Get a 4D matrix for all the correlations
def getComplexValues(pdata, station, corrs , polar, cords, ntimes, nfreqs):
    corrData = []
    # We read the available correlations
    for corr in CORRS_NAMES:
        if corr in corrs:
            corrData.append(getComplexCorrelationValues(pdata, station, corr , polar, cords).tolist())
        else:
            # If this correlation is not available we fill it with -1000
            # assuming same dimensions than rest of data
            corrData.append((numpy.zeros((nfreqs,ntimes,2)) - 1000.).tolist())
    return corrData

def hasGain(absPath):
    return os.path.isdir(absPath + '/' + GAIN_TAB_NAME)

# Get gain data using the parmdb
def getGain(absPath):
    gainTableAbsPath = absPath + '/' + GAIN_TAB_NAME
    p = 0
    try:
        if os.path.isdir(gainTableAbsPath):
            p = pdb.parmdb(gainTableAbsPath)
            # Get the data as sampled in the database 
            pdata = p.getValuesGrid('*')
            names = pdata.keys()
            if len(names):
                
                # Get which stations, correlations and coordinates we have in the file
                corrSet = set([])
                stationSet = set([])
                cordSet = set([])
                for name in names:
                    sname = name.split(':')
                    # Add the station and correlation to its sets
                    corrSet.add(sname[1] + ':' + sname[2])
                    stationSet.add(sname[4])
                    cordSet.add(sname[3])
                    # Check that we are handling only Gain or TEC cases
                    if sname[0] != GAIN_TYPE:
                        raise Exception(' not ' + GAIN_TYPE)
                
                # Check if given data is in polar or not
                if REAL_COORD in cordSet or IMAG_COORD in cordSet:
                    # In this case if one of them is missing we just assume is all 0s
                    polar = False
                elif AMPL_COORD in cordSet and PHASE_COORD in cordSet:
                    # In this case we require both coordinates
                    polar = True
                else:
                    raise Exception('only contains ' + ','.join(cordSet))
                
                # Check if we have necessary correlations
                isValid = False
                for corr in corrSet:
                    if corr in CORRS_NAMES:
                        isValid = True
                if not isValid:
                    raise Exception('must contain at least one of ' + ','.join(CORRS_NAMES))
                
                # Make lists of the set objects
                stations = sorted(list(stationSet))
                cords = list(cordSet)
                corrs = sorted(list(corrSet))
                
                # Assume all the stations have the same steps and dimensions
                sampledatarow = pdata[names[0]]
                timewidths = sampledatarow['timewidths'] 
                timestep = timewidths.mean()
                ntimes = len(timewidths)
                freqwidths = sampledatarow['freqwidths']
                freqstep = freqwidths.mean()
                nfreqs = len(freqwidths)
                
                direction = (-1.,-1.)
                data = {}
                for i in range(len(stations)):
                    data[(stations[i],direction)] = getComplexValues(pdata, stations[i], corrs , polar, cords, ntimes, nfreqs)
                p = 0
                return (freqstep, timestep, data)
    except Exception,e:
        print 'ERROR getting gain from ' + gainTableAbsPath + ': ' + str(e)
    # Close the paramdb
    p = 0      
    return None

#
# QUALITY CODE
#
# Get step of TIME or FREQUENCY
def getStep(absPath, tableAbsPath, column):
    values = pt.taql('select DISTINCT ' + column + ' from ' + tableAbsPath + ' ORDERBY ' + column)
    nvalues = values.nrows()
    step = 0.
    if nvalues == 1:
        table = msoperations.getTable(absPath)
        if column == 'FREQUENCY':
            step = msoperations.getTotalBandwith(table) * 1.e6
        else: #TIME
            (tini,tend) = msoperations.getEpochMJD(table, absPath)
            step = tend-tini
        table.close()
    elif nvalues > 1:
        step = values.getcell(column,1) - values.getcell(column,0)
    return step

# Return the quality kinds of the ms indicated by absPath
# It retruns a tuple, which first element is the array of IDs of the quality
# kind and the second is its names
def getQualityKinds(absPath):
    table = pt.table(absPath, readonly=True, ack=False)
    qKTab = pt.table(table.getkeyword('QUALITY_KIND_NAME'), readonly=True, ack=False)
    (qKs,qKNames) = (qKTab.getcol('KIND'). tolist(),qKTab.getcol('NAME'))
    qKTab.close()
    table.close()
    return (qKs,qKNames)

def hasQuality(absPath):
    return os.path.isdir(absPath + '/' + QTS_TAB_NAME)

# Get qts diag data
def getQTS(absPath):
    qTimeStatTableAbsPath = absPath + '/' + QTS_TAB_NAME
    try:
        if os.path.isdir(qTimeStatTableAbsPath):
            # Get the quality kind names
            (qKs,qKNames) = getQualityKinds(absPath)
            # Get the steps in freqency and time
            freqStep = getStep(absPath, qTimeStatTableAbsPath, 'FREQUENCY')
            timeStep = getStep(absPath, qTimeStatTableAbsPath, 'TIME')
            data = []
            for i in range(len(qKs)):
                tK = pt.taql('select * from ' + qTimeStatTableAbsPath + ' where KIND = %d ORDERBY TIME' % (qKs[i],))
                chsData = []
                for tch in tK.iter(['FREQUENCY']):
                    chsData.append(tch.getcol('VALUE'))
                data.append(numpy.array(chsData))
            data = numpy.array(data)
            # Create array with separated real and imag ([comp][qk][freq][time][pol])
            # and transpose it in order to have [qK][pol][freq][time][compl]
            return (freqStep, timeStep, qKNames, numpy.array([data.real,data.imag]).transpose((1,4,2,3,0)).tolist())
    except Exception,e:
        print 'ERROR getting quality time statistic from ' + qTimeStatTableAbsPath + ': ' + str(e)      
    return None

# Get qfs diag data
def getQFS(absPath):
    qFreqStatTableAbsPath = absPath + '/' + QFS_TAB_NAME
    try:
        if os.path.isdir(qFreqStatTableAbsPath):
            # Get the quality kind names
            (qKs,qKNames) = getQualityKinds(absPath)
            # Get the step in frequency
            freqStep = getStep(absPath, qFreqStatTableAbsPath, 'FREQUENCY')
            data = []
            for i in range(len(qKs)):
                data.append(pt.taql('select VALUE from ' + qFreqStatTableAbsPath + ' where KIND = %d ORDERBY FREQUENCY' % (qKs[i],)).getcol('VALUE'))
            data = numpy.array(data)
            # Create array with separated real and imag ([comp][qk][freq][pol])
            # and transpose it in order to have [qK][pol][freq][compl]
            return (freqStep, qKNames, numpy.array([data.real,data.imag]).transpose((1,3,2,0)).tolist())
    except Exception,e:
        print 'ERROR getting quality frequency statistic from ' + qFreqStatTableAbsPath + ': ' + str(e)      
    return None

# Get qbs diag data
def getQBS(absPath):
    qBaseStatTableAbsPath = absPath + '/' + QBS_TAB_NAME
    try:
        if os.path.isdir(qBaseStatTableAbsPath):
            (qKs,qKNames) = getQualityKinds(absPath)
            freqStep = getStep(absPath, qBaseStatTableAbsPath, 'FREQUENCY')
            table = pt.table(absPath, readonly=True, ack=False)
            stationNames = pt.table(table.getkeyword('ANTENNA'), readonly=True, ack=False).getcol('NAME')
            table.close()
            data = []
            storeBase = True
            baselines = []
            for i in range(len(qKs)):
                tK = pt.taql('select * from ' + qBaseStatTableAbsPath + ' where KIND = %d ORDERBY FREQUENCY,ANTENNA1,ANTENNA2' % (qKs[i],))
                chsData = []
                for tch in tK.iter(['FREQUENCY']):
                    chData = []
                    for j in range(tch.nrows()):
                        chData.append(tch.getcell('VALUE',j))
                        if storeBase:
                            (ant1,ant2) = sorted((tch.getcell('ANTENNA1',j),tch.getcell('ANTENNA2',j)))
                            baselines.append((stationNames[ant1],stationNames[ant2]))
                    storeBase = False
                    chsData.append(chData)
                data.append(chsData)
            data = numpy.array(data)
            # Create array with separated real and imag ([comp][qk][freq][baseline][pol])
            # and transpose it in order to have [qK][baseline][pol][freq][compl]
            return (freqStep, qKNames, baselines, numpy.array([data.real,data.imag]).transpose((1,3,4,2,0)).tolist())
    except Exception,e:
        print 'ERROR getting quality baseline statistic from ' + qBaseStatTableAbsPath + ': ' + str(e)      
    return None

def getPartitionSchema(connection):
    """Get the partition schema, i.e. all the partitions of the DiagTable and the node point of the contained MSs"""
    diagTable = GAIN # all diag should have same partitions 
    cursor = connection.cursor()
    createLEDDBPartitions = CreateLEDDBPartitions()
    cursor.execute('select tablename from pg_tables where tablename ~ %s order by tablename', [diagTable.lower()+"_part_*",])
    partNames = cursor.fetchall()
    numParts = len(partNames)
    partTablesSuffixes = []
    nodePoints = []
    for i in range(numParts):
        partTablesSuffixes.append(partNames[i][0].replace(diagTable.lower(),''))
        (lowerMSId, upperMSId) = createLEDDBPartitions.getPartitionDescription(partNames[i][0])[1:]
        if i == 0:
            nodePoints.append(lowerMSId)
        nodePoints.append(upperMSId)
    cursor.close()
    return (partTablesSuffixes, nodePoints)

def getPartition(diagTable, partTablesSuffixes, nodePoints, msId):
    if len(partTablesSuffixes) == 0:
        # there are not partitions 
        return diagTable
    partIndex = bisect.bisect(nodePoints, msId) - 1
    if partIndex < 0:
        raise Exception('ERROR: could not find related partition table for MSID = ' + str(msId))
    if partIndex >= len(partTablesSuffixes):
        raise Exception('ERROR: could not find related partition table for MSID = ' + str(msId) + '. We need to create more partition tables!')
    return diagTable + partTablesSuffixes[partIndex]