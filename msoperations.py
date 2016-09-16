#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
import subprocess
import re
import numpy
import numpy.ma
from ledama import config as lconfig
from ledama import utils
try:
        import pyrap.tables as pt
except ImportError:
        print "Error: The pyrap tables module is not available."
import ledama.leddb.LDSMissingInformation as LDSMissingInformation
        
# This module define some operations from a MS
# IMPORTANT: All the methods returning a number or string return None some error
# happened.

def getMeasurementSetName(absPath):
    """Get the measurement set name from its path"""
    return absPath.split('/')[-1]   

def getParentPath(absPath):
    """ Get the parent path of the MS path """
    return absPath.replace('/'+getMeasurementSetName(absPath),'')

def getStore(hostName, absPath):
    """ Get the store name from the path and a host name"""
    if hostName.count('node'):
        if absPath.count('backup'):
            return utils.EORBACKUP
        else:
            return utils.EOR
    else:
        return hostName
           
def getLDSName(absPath):
    """ Get the LDS name from the MS path. 
    In LOFAR EoR cluster the convention is too use as parent path of
    the MSs the following: LXXXX_YYYYY[_ZZZ] or LYYYYY[_ZZZ] where:
      - XXXX is the year where the observation was took [ 4 digits ] 
      - YYYYY is the number of the LDS name [at least 5 digits] LDS name = LYYYYY
      - ZZZ is the version and it is optional """
    f = re.search('L[0-9][0-9][0-9][0-9]_[0-9]*', absPath)
    if f != None:
        return 'L' + (str(f.group(0))).split('_')[-1]
    else:
        f = re.search('L[0-9]*', absPath)
        if f != None:
            return str(f.group(0))
    return None

def getVersionIndex(absPath):
    """ Gets the version index from the MS path """
    f = re.search('L[0-9][0-9][0-9][0-9]_[0-9]*_[0-9][0-9][0-9]', absPath)
    if f != None:
        return int(str(f.group(0)).split('_')[-1])
    else:
        f = re.search('L[0-9]*_[0-9][0-9][0-9]/', absPath)
        if f != None:
            return int(str(f.group(0)).split('_')[-1].replace('/',''))
        else:
            f = re.search('L[0-9]*_[0-9][0-9][0-9]_', absPath)
            if f != None:
                return int(str(f.group(0)).split('_')[1])
    return 0
    
def getSBIndex(absPath):
    """ Get the SB index (integer) of the MS from its path"""
    f = re.search('SB[0-9][0-9][0-9]', absPath)
    if f != None:
        return int((str(f.group(0))).replace('SB',''))
    else:
        f = re.search('sb[0-9][0-9][0-9]', absPath)
        if f != None:
            return int((str(f.group(0))).replace('sb',''))
        return None    
    
def getBeamIndex(absPath):
    """ Get the beam index its path"""
    f = re.search('SAP[0-9][0-9][0-9]', absPath)
    if f != None:
        return int((str(f.group(0))).replace('SAP',''))
    else:
        ldsName = getLDSName(absPath) 
        if ldsName == 'L25662':
            return (getSBIndex(absPath) / 34)
        elif ldsName == 'L22120':
            return (getSBIndex(absPath) / 62)
        else:
            return 0  

def getSize(absPath):
    """ Get the size of a MS """
    try:
        if os.path.islink(absPath):
            return int(((os.popen('du -sm ' + os.readlink(absPath))).read().split('\t'))[0])
        else:
            return int(((os.popen('du -sm ' + absPath)).read().split('\t'))[0])
    except ValueError:
        return None

def isTar(absPath):
    """ Check is the MS is TAR """
    return absPath.lower().endswith('.tar')

def isBvf(absPath):
    """ Check is the MS is BVF """
    return absPath.lower().endswith('.bvf')

def isPacked(absPath):
    """ Check if the MS is packed, i.e. is a TAR or a BVF file """
    return isBvf(absPath) or isTar(absPath)

def isRaw(absPath):
    """ Check if a MS is raw (we assume it is raw if the name does not contain a ".dppp" string """
    return (absPath.lower().count('dppp') == 0)

def getYear(absPath):
    """ Get the year of the observation """
    # First we try to get it form the path
    f = re.search('L[0-9][0-9][0-9][0-9]_[0-9]*', absPath)
    if f != None:
        return f.group(0).split('_')[0].replace('L','')
    try:
        tini = utils.removeDecimal(str((pt.taql('calc ctod(mjdtodate([select TIME from ' + absPath + ' limit 1 offset 1]))'))['0'][0]))
        return utils.changeTimeFormat(tini, finalTimeFormat = "%Y")
    except:
        try:
            tini = utils.removeDecimal(str((pt.taql('calc ctod(mjdtodate([select TIME from ' + absPath + ' limit 1 offset 1]))'))['array'][0]))
            return utils.changeTimeFormat(tini, finalTimeFormat = "%Y")
        except:
            return None
    
def getTable(absPath):
    """ Get the pyrap table of a MS (with read-only access) """
    return pt.table(absPath, readonly=True, ack=False)

def getProject(ms, absPath):
    """ Get the related project name of the observation of which the MS is part """
    try:
        tobs = pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False)
        project = tobs.getcell('PROJECT',0)
        if project == 'unknown' or project == '':
            return LDSMissingInformation.getProject(getLDSName(absPath))
        return project
    except:
        return LDSMissingInformation.getProject(getLDSName(absPath))

def getField(ms, absPath):
    """ Get the field name of the observation of which the MS is part"""
    try:
        tobs = pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False)
        field = ','.join(tobs.getcell('LOFAR_TARGET',0))
        if field == '':
            return LDSMissingInformation.getField(getLDSName(absPath))
        return field
    except:
        return LDSMissingInformation.getField(getLDSName(absPath))

def getAntennaSet(ms, absPath):
    """ Get the antenna set used during the observation of which the MS is part"""
    try:
        tobs = pt.table(ms.getkeyword('OBSERVATION'), readonly=True, ack=False)
        return tobs.getcell('LOFAR_ANTENNA_SET',0)
    except:
        return LDSMissingInformation.getAntennaSet(getLDSName(absPath))


def getEpochUTC(ms, absPath):
    """This function get a string with the epoch of the MS in UTC (first and last time sample value)"""
    try:
        lds = getLDSName(absPath)
        if lds == 'L52037':
            return ('2012/03/13/02:31:00', '2012/03/13/03:01:00')
        else:
            try:
                tini = (pt.taql('calc ctod(mjdtodate([select TIME from ' + absPath + ' limit 1 offset 1]))'))['0'][0]
                tend = (pt.taql('calc ctod(mjdtodate([select TIME from ' + absPath + ' limit 1 offset ' + str(ms.nrows() - 1) + ']))'))['0'][0]
            except:
                tini = (pt.taql('calc ctod(mjdtodate([select TIME from ' + absPath + ' limit 1 offset 1]))'))['array'][0]
                tend = (pt.taql('calc ctod(mjdtodate([select TIME from ' + absPath + ' limit 1 offset ' + str(ms.nrows() - 1) + ']))'))['array'][0]
            return (utils.removeDecimal(str(tini)), utils.removeDecimal(str(tend)))
    except:
        return (None,None)

def getEpochMJD(ms, absPath):
    """This function get the epoch of the MS in MJD (first and last time sample value)"""
    try:
        lds = getLDSName(absPath)
        if lds == 'L52037':
            return (4838322659.9039869, 4838324459.6160116)
        else:
            tini = ms.getcell('TIME',0)
            tend = ms.getcell('TIME',ms.nrows()-1)
            return (tini,tend)
    except:
        return (None,None)
         
def getAntennaType(ms):
    """ This function gets the frequency band (HBA or LBA) of a MS """
    try:
        tant = pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False)
        antList = tant.getcol('NAME')
        for i in range(len(antList)):
            for band in ('HBA', 'LBA'):
                if antList[i].count(band):
                    return band
        return None
    except:
        return None

def getStations(ms):
    """ Get the list of stations names used in the observation of which the MS is part """
    try:
        tant = pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False)
        antList = tant.getcol('NAME')
        return antList
    except: 
        return None

def getStationsPositions(ms):
    """ Get the list of stations positions used in the observation of which the MS is part """
    try:
        tant = pt.table(ms.getkeyword('ANTENNA'), readonly=True, ack=False)
        antPosList = tant.getcol('POSITION')
        return antPosList
    except: 
        return None
    
def getStationLocationType(stationName):
    """Get the station location type, i.e a string that can be CS (core station), RS (remote station) or IS (international station)"""
    if stationName.count('CS'):
        return 'CS'
    elif stationName.count('RS'):
        return 'RS'
    else:
        return 'IS'

def getIntegrationTime(ms):
    """ This function get the integration time of a MS"""
    try:
        return ms.getcell("INTERVAL", 0)
    except:
        return -1.
    
def getNumberOfChannels(ms):
    """ This function get the number of channels of a MS """
    try:
        tsp = pt.table(ms.getkeyword('SPECTRAL_WINDOW'), readonly=True, ack=False)
        return len(tsp.getcell('CHAN_FREQ',0))
    except:
        return -1
 
def getApplications(ms):
    """ Get the list with the applications names that has processed somehow the MS"""
    try:
        thistory = pt.table(ms.getkeyword('HISTORY'), readonly=True, ack=False)
        appList = []
        
        for app in thistory.getcol('APPLICATION'): 
            if app != 'imager':
                appList.append(app)
        return appList
    except:
        return []
    
def getPipelineDescr(applications):
    """ Get 3 lists of the applications (and its orders) that has processed the 
    MS. First list is for aoflagger runs, second for NDPPP and third for BBS """
    flagApps = []
    averApps = []
    calApps = []
    for index in range(len(applications)):
        app = applications[index]
        if app == 'AOFlagger':
            flagApps.append(str(index) + ':' + app)
        elif app == 'NDPPP':
            averApps.append(str(index) + ':' + app)
        elif app == 'BBS':
            calApps.append(str(index) + ':' + app)
    return (flagApps,averApps,calApps)

def getCentralFrequency(ms):
    """ Get the central frequency of a MS"""
    try:
        tsp = pt.table(ms.getkeyword('SPECTRAL_WINDOW'), readonly=True, ack=False)
        return tsp.getcell('REF_FREQUENCY',0)/1.e6
    except:
        return None
    
def getTotalBandwith(ms):
    """ Get the total bandwidth of a MS"""
    try:
        tsp = pt.table(ms.getkeyword('SPECTRAL_WINDOW'), readonly=True, ack=False)
        return tsp.getcell('TOTAL_BANDWIDTH',0)/1.e6
    except:
        return None

def getPhaseDirection(ms):
    """ Get the phase (pointing) direction of the observation of which the MS is part"""
    try:
        tfield = pt.table(ms.getkeyword('FIELD'), readonly=True, ack=False)
        a = tfield.getcol('DELAY_DIR')
        return ((180. * (a[0][0][0] / numpy.pi)) , (180. * (a[0][0][1] / numpy.pi))) 
    except:
        return (None, None)
    
def addImagingColumns(absPath, node):
    """ This function add the imaging columns to a MS """
    if os.path.isdir(absPath):
        try:
            pt.addImagingColumns(absPath)
            return getSize(absPath)
        except Exception, e:
            return str(e)

def cleanBand(absPath, dataColumn):
    """ This function remove a column from a MS """
    # We check that this node contain pathToCheck 
    if os.path.isdir(absPath):
        try:
            # We use pyrap to get the table
            t = pt.table(absPath, readonly=False, ack=False)
            # Remove the data
            if dataColumn in t.colnames():
                t.removecols(dataColumn)
            else:
                return dataColumn + ' column is not in ' + absPath
            t.flush()
            t.close()
            return getSize(absPath)
        except Exception, e:
            return str(e)
        
def getData(table,column):
    """ Get the column data indicated bu column. It also accepts cola+colb or cola-colb """
    splitColumn = column.split(',')
    if len(splitColumn) == 1:
        return table.getcol(column)
    elif len(splitColumn) == 3:
        columnAData = table.getcol(splitColumn[0])
        operation = splitColumn[1]
        columnBData = table.getcol(splitColumn[2])
        
        if operation == '-':
            return (columnAData-columnBData)
        elif operation == '+':
            return (columnAData+columnBData)
    # If we reach this point it means that the column is not correct
    print 'Column to plot: ' + column + ' is not correct!'
    return None
    
def getXAxisVals(table, axisname, channels):
    """ Get the x axis data out of the table (time, ha, chan or freq). In case chan or freq we also need to specify which channels"""    
    if axisname == 'time':
        tmp = table.getcol('TIME')
        if tmp != None:
            return numpy.array(tmp-tmp.min(),dtype=numpy.float)
        return None
    if axisname == 'ha':
        tmp = table.getcol('HA')
        if tmp!= None:
            return numpy.array(tmp*180./numpy.pi*3600./15.,dtype=numpy.float)
        return None
    elif axisname == 'chan':
        tmp = table.getcol('CHAN_FREQ')[0]
        if tmp != None:
            return numpy.array(range(len(tmp))[channels[0]:channels[1]],dtype=numpy.float)+1
        return None
    elif axisname == 'freq':
        tmp = table.getcol('CHAN_FREQ')[0]
        if tmp != None:
            return numpy.array(tmp[channels[0]:channels[1]],dtype=numpy.float)/1.e6
        return None
    else:
        print 'Error: Requested axis not implemented'
        return None

def getCorrData(data, polindex):
    """ Get the data os a certain polarization. It check if the data matrix is 2D or 3D.
    By default it should be 3D but if we did some averaging in time or freq then is 2D"""
    if len(data.shape) == 3:
        return data[:, :, polindex]
    else:
        #some average has been done in time or freq
        return data[:, polindex]
 
def get3DCutData(table, column, showFlags, flagCol, channels, stokes):
    """  Gets the data from the indicated column with all correlations and maybe
     a cut in channels. Convert to stokes and set the flags if required """
    # Get the data 
    tmp0 = getData(table, column)
    if tmp0 != None:
        if showFlags:
            # If the flag data must be shown we ignore the flag mask
            tmp1 = numpy.ma.array(tmp0, dtype=None, mask=False)
        else:
            tmp1 = numpy.ma.array(tmp0, dtype=None, mask=table.getcol(flagCol))
        # Flag the nan values
        tmp1[numpy.isnan(tmp1)]=numpy.ma.masked
        
        # Convert to stokes
        if stokes:
            tmp2 = numpy.ma.transpose(numpy.ma.array([tmp1[:,:,0]+tmp1[:,:,3],tmp1[:,:,0]-tmp1[:,:,3],tmp1[:,:,1]+tmp1[:,:,2],numpy.complex(0.,-1.)*(tmp1[:,:,2]-tmp1[:,:,1])],dtype=None,mask=tmp1.mask),(1,2,0))
        else:
            tmp2 = tmp1
            
        # this contains the cut in channels desired by the user
        return tmp2[:,channels[0]:channels[1],:]
     
def getAvgData(cutdata, prefaxis=0):
    """ Average the data over certain axis, prefaxis tells us in which axis we 
    are interested (the other will be averaged). 
    If prefaxis is None, none axis is averaged """
    if cutdata is None:
        print 'Error: selected data'
        return None
    # We average the axis if needed
    if prefaxis == 0:
        return numpy.ma.mean(cutdata,axis=1) # 2D
    elif prefaxis == 1:
        return numpy.ma.mean(cutdata,axis=0) # 2D
    else:
        # No average over axis
        return cutdata # 3D     

def getAvgDataOperation(cutdata, operation, prefaxis=0):
    """ Get the data related to a certain operation over the correlations (2D or 1D of float numbers) """
    intdata = getAvgData(cutdata,prefaxis)
    if operation == 1:
        # Special operation : XX - YY
        return getCorrData(intdata, 0) - getCorrData(intdata, 3)
    elif operation == 2:
        # Special operation : XY . YX*
        return getCorrData(intdata, 1) * getCorrData(intdata, 2).conjugate()
    else:
        print 'Error: Requested operation not implemented'
        return None   
    
def getComplexAvgCompData(intdata, complexcomp, unwrap):
    """ Get a complex coordinate (real, imag, amp, phase or phase_rate) of the data  """
    if complexcomp == 'real':
        return intdata.real
    elif complexcomp == 'imag':
        return intdata.imag
    elif complexcomp == 'amp':
        return numpy.ma.array(numpy.abs(intdata),mask=intdata.mask)
    elif complexcomp.count('phase'):
        phaseop = numpy.angle(intdata)
        if complexcomp == 'phase':
            if unwrap:
                phaseop = numpy.unwrap(phaseop,axis=0)
            return numpy.ma.array(phaseop, mask=intdata.mask)
        else:
            # phase_rate
            phaseop = numpy.unwrap(phaseop,axis=0)
            tmp=numpy.diff(phaseop,axis=0)
            return numpy.ma.array(numpy.append(tmp,numpy.array([tmp[-1],]),axis=0), mask=intdata.mask)
    
def getStats(intData, compCoordDict, statparams, doUnwrap):
    """ Get the statistic (mean, median or std) of the data. 
    compCoordDict and statparams indicate the order and type of demanded stats"""
    statresults = []
    for i in range(len(statparams)):
        statresults.append(float('nan'))
    if intData.count():
        for complexcomponent in compCoordDict:
            complexIntCompData = getComplexAvgCompData(intData, complexcomponent, doUnwrap)
            indexes = compCoordDict[complexcomponent]
            for index in indexes:
                statistic = statparams[index]
                if statistic == 'mean':
                    statresults[index] = complexIntCompData.mean()
                elif statistic == 'median':
                    statresults[index] = numpy.ma.median(complexIntCompData)
                elif statistic == 'std':
                    statresults[index] = complexIntCompData.std()
    return statresults
    
def isUnused(absPath):
    """Check whether a MS is being used by checking its table lock"""
    initCommand = ''
    if lconfig.INIT_FILE != '':
        initCommand = 'source ' + lconfig.INIT_FILE + ' ;'
    (out,err) = subprocess.Popen(initCommand + 'showtablelock ' + absPath, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    # If the tabel si locked we will see two possible messages:
    #  [tab] is write-locked in process XXXX or 
    #  [tab] is read-locked in process XXXX or
    return (out.count('-locked') == 0)