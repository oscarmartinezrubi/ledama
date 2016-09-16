#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, sys, time, calendar, math, numpy, pwd
import ledama.config as lconfig
# This script defines some util methods
SARA = 'SARA'
JUELICH = 'Juelich'
SOUTHAMPTON = 'SOUTHAMPTON'
TARGETSCRATCH='TARGET_SCRATCH'
TARGET_E_OPS='TARGET_E_OPS'
TARGET_E_EOR='TARGET_E_EOR'
TARGET_F_EOR='TARGET_F_EOR'

REMOTE_STORES = [SARA, JUELICH, SOUTHAMPTON, TARGETSCRATCH, TARGET_E_OPS,TARGET_E_EOR, TARGET_F_EOR ]

EOR = 'EoR'
EORBACKUP = 'EoRBackup'

ALL_STORES = [EOR, EORBACKUP, SARA, JUELICH, SOUTHAMPTON, TARGETSCRATCH, TARGET_E_OPS,TARGET_E_EOR, TARGET_F_EOR ]

# Time format for time stamps
TIMEFORMAT = "%Y/%m/%d/%H:%M:%S"
DAYLY_TIMEFORMAT = "%H:%M:%S"

# Paths for running the code
SOFTWAREPATH = lconfig.LEDAMA_ABS_PATH.replace('ledama', '')

PCOLORS = [(0.0, 0.0, 1.0), (0.0, 1.0, 0.0), (1.0, 0.0, 0.0), (0.0, 0.75, 0.75), 
           (0.75, 0.0, 0.75), (0.75, 0.75, 0.0), (0.0, 1.0, 1.0),
           (1.0, 1.0, 0.0), (1.0, 1.0, 1.0), (0.0, 0.0, 0.25), (1.0, 0.0, 1.0), (0.0, 0.0, 0.5), (0.0, 0.0, 0.75), 
           (0.0, 0.25, 0.0), (0.0, 0.25, 0.25), (0.0, 0.25, 0.5), (0.0, 0.25, 0.75), (0.0, 0.25, 1.0), 
           (0.0, 0.5, 0.0), (0.0, 0.5, 0.25), (0.0, 0.5, 0.5), (0.0, 0.5, 0.75), (0.0, 0.5, 1.0), 
           (0.0, 0.75, 0.0), (0.0, 0.75, 0.25), (0.0, 0.75, 0.5), (0.0, 0.75, 1.0), (0.0, 1.0, 0.25), 
           (0.0, 1.0, 0.5), (0.0, 1.0, 0.75), (0.25, 0.0, 0.0), (0.25, 0.0, 0.25), (0.25, 0.0, 0.5), 
           (0.25, 0.0, 0.75), (0.25, 0.0, 1.0), (0.25, 0.25, 0.0), (0.25, 0.25, 0.25), (0.25, 0.25, 0.5), 
           (0.25, 0.25, 0.75), (0.25, 0.25, 1.0), (0.25, 0.5, 0.0), (0.25, 0.5, 0.25), (0.25, 0.5, 0.5), 
           (0.25, 0.5, 0.75), (0.25, 0.5, 1.0), (0.25, 0.75, 0.0), (0.25, 0.75, 0.25), (0.25, 0.75, 0.5), 
           (0.25, 0.75, 0.75), (0.25, 0.75, 1.0), (0.25, 1.0, 0.0), (0.25, 1.0, 0.25), (0.25, 1.0, 0.5),
            (0.25, 1.0, 0.75), (0.25, 1.0, 1.0), (0.5, 0.0, 0.0), (0.5, 0.0, 0.25), (0.5, 0.0, 0.5),
             (0.5, 0.0, 0.75), (0.5, 0.0, 1.0), (0.5, 0.25, 0.0), (0.5, 0.25, 0.25), (0.5, 0.25, 0.5), 
             (0.5, 0.25, 0.75), (0.5, 0.25, 1.0), (0.5, 0.5, 0.0), (0.5, 0.5, 0.25), (0.5, 0.5, 0.5), 
             (0.5, 0.5, 0.75), (0.5, 0.5, 1.0), (0.5, 0.75, 0.0), (0.5, 0.75, 0.25), (0.5, 0.75, 0.5), 
             (0.5, 0.75, 0.75), (0.5, 0.75, 1.0), (0.5, 1.0, 0.0), (0.5, 1.0, 0.25), (0.5, 1.0, 0.5), 
             (0.5, 1.0, 0.75), (0.5, 1.0, 1.0), (0.75, 0.0, 0.0), (0.75, 0.0, 0.25), (0.75, 0.0, 0.5), 
             (0.75, 0.0, 1.0), (0.75, 0.25, 0.0), (0.75, 0.25, 0.25), (0.75, 0.25, 0.5), (0.75, 0.25, 0.75),
              (0.75, 0.25, 1.0), (0.75, 0.5, 0.0), (0.75, 0.5, 0.25), (0.75, 0.5, 0.5), (0.75, 0.5, 0.75), 
              (0.75, 0.5, 1.0), (0.75, 0.75, 0.25), (0.75, 0.75, 0.5), (0.75, 0.75, 0.75), (0.75, 0.75, 1.0), 
              (0.75, 1.0, 0.0), (0.75, 1.0, 0.25), (0.75, 1.0, 0.5), (0.75, 1.0, 0.75), (0.75, 1.0, 1.0), (1.0, 0.0, 0.25), 
              (1.0, 0.0, 0.5), (1.0, 0.0, 0.75), (1.0, 0.25, 0.0), (1.0, 0.25, 0.25), (1.0, 0.25, 0.5), (1.0, 0.25, 0.75), 
              (1.0, 0.25, 1.0), (1.0, 0.5, 0.0), (1.0, 0.5, 0.25), (1.0, 0.5, 0.5), (1.0, 0.5, 0.75), (1.0, 0.5, 1.0), 
              (1.0, 0.75, 0.0), (1.0, 0.75, 0.25), (1.0, 0.75, 0.5), (1.0, 0.75, 0.75), (1.0, 0.75, 1.0), (1.0, 1.0, 0.25), 
              (1.0, 1.0, 0.5), (1.0, 1.0, 0.75),]

 
PCOLORS_ALT =  ["#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF", "#00FFFF", "#000000", 
        "#800000", "#008000", "#000080", "#808000", "#800080", "#008080", "#808080", 
        "#C00000", "#00C000", "#0000C0", "#C0C000", "#C000C0", "#00C0C0", "#C0C0C0", 
        "#400000", "#004000", "#000040", "#404000", "#400040", "#004040", "#404040", 
        "#200000", "#002000", "#000020", "#202000", "#200020", "#002020", "#202020", 
        "#600000", "#006000", "#000060", "#606000", "#600060", "#006060", "#606060", 
        "#A00000", "#00A000", "#0000A0", "#A0A000", "#A000A0", "#00A0A0", "#A0A0A0", 
        "#E00000", "#00E000", "#0000E0", "#E0E000", "#E000E0", "#00E0E0", "#E0E0E0"]


def getHome(userName = None):
    if userName == None:
        return lconfig.HOME_PARENT_PATH + '/' + getUserName()
    else:
        return lconfig.HOME_PARENT_PATH +  '/' + userName
    
def getDefClusDesc():
    return lconfig.DEF_CLUSTER_DESCRIPTION_FILE

def getCurrentTime(t = None):
    """
    Get current localUnixTime if t is None. Convert t to localUnixTime. 
    t is seconds since the epoch given by module time (GMT/UTC)
    localUnixTime = UnixTime + 7200 [or 3600]
    """
    return calendar.timegm(time.localtime(t))

def getCurrentUTCTime():
    """
    Get current UTC UnixTime
    """
    return calendar.timegm(time.gmtime())

def getCurrentTimeStamp(timeFormat = TIMEFORMAT):
    """ 
    Get current local time stamp 
    """
    return str(time.strftime(timeFormat))

def getCurrentUTCTimeStamp(timeFormat = TIMEFORMAT):
    """ 
    Get current UTC/GMT time stamp 
    """
    return time.strftime(timeFormat,time.gmtime())

def convertTimeStamp(timeStamp, timeFormat = TIMEFORMAT):
    """
    Get the secs since epoch given by module time for a timeStamp
    So, it converts GMT/UTC to UnixTime or localTimeStamp to localUnixTime
    If timeFormat is DAILY_TIMEFORMAT it returns (hh*3600)+(mm*60)+ss
    """
    if timeFormat == DAYLY_TIMEFORMAT:
        [hh,mm,ss] = timeStamp.split(':')
        return (int(hh) * 3600) + (int(mm) * 60) + int(ss)
    else:
        return calendar.timegm(time.strptime(timeStamp, timeFormat))
    
def getTimeStamp(t, timeFormat = TIMEFORMAT):
    """
    Get the time stamp from t assuming t is seconds since the epoch given by module time (GMT)
    So, it converts UnixTime to GMT/UTC or localUnixTime to localTimeStamp 
    """
    if timeFormat == DAYLY_TIMEFORMAT:
        hh = (t / 3600) % 24
        rh = t % 3600
        mm = rh / 60
        ss = rh % 60
        return ('%02d' % hh) + ':' + ('%02d' % mm) + ':' + ('%02d' % ss)
    else:
        return str(time.strftime(timeFormat, time.gmtime(t)))
    
def changeTimeFormat(timeStamp, initialTimeFormat = TIMEFORMAT, finalTimeFormat = DAYLY_TIMEFORMAT):
    """
    Change the format of a timeStamp
    """
    if initialTimeFormat == TIMEFORMAT and finalTimeFormat == DAYLY_TIMEFORMAT:
        return timeStamp.split('/')[-1]
    else:
        return getTimeStamp(convertTimeStamp(timeStamp, initialTimeFormat), finalTimeFormat)
# Returns ok if difference between t2 and t1 is higher than delta 
def isTimeStampOlder(timeStamp1, timeStamp2, delta, timeFormat = TIMEFORMAT):
    if math.fabs(convertTimeStamp(timeStamp2, timeFormat) - convertTimeStamp(timeStamp1, timeFormat)) >= delta:
        return True
    else:
        return False

# Get the last modification of a file or folder (check recursively in possible folder contents)
def getLastModification(absPath, initialLMTime = None):
    """
    Get the last modification time of the provided path. 
    The returned values is the number of seconds since the epoch given in time module
    So, it is the UnixTime
    """
    lastModifiedTime = initialLMTime
    if os.path.isfile(absPath):
        t = os.path.getmtime(absPath)
        if lastModifiedTime == None or t > lastModifiedTime:
            lastModifiedTime = t
    elif os.path.isdir(absPath):
        # it is a dir
        for element in os.listdir(absPath):
            elementPath = absPath + '/' + element
            t = os.path.getmtime(elementPath)
            if lastModifiedTime == None or t > lastModifiedTime:
                lastModifiedTime = t
            if os.path.isdir(elementPath):
                t = getLastModification(elementPath, lastModifiedTime)
                if lastModifiedTime == None or t > lastModifiedTime:
                    lastModifiedTime = t
    return lastModifiedTime
    
def removeDecimal(timeStamp):
    return timeStamp.split('.')[0]

def UTC2LocalTime(utcTimeStamp, timeFormat = TIMEFORMAT):
    """
    Convert a GMT/UTC time stamp to a localTimeStamp 
    """
    return str(time.strftime(timeFormat, time.localtime(calendar.timegm(time.strptime(removeDecimal(utcTimeStamp), timeFormat)))))

def getUserName():
    return os.popen('whoami').read().replace('\n','')

# Get the current node name
def getHostName():
    return (os.popen("'hostname'")).read().split('\n')[0]
    
# Format the user name to fit in 8 characters
def formatUserName(userName):
    if len(userName) > 8:
        return (userName[0:8])
    else:
        return userName

# Gets a list of the elements of the  rangeString. For example for a rangeString
# 0,1,5-10,12,14-16 We would get a list as:
# 0,1,5,6,7,8,9,10,12,14,15,16
# it return None if we detect any unexpected format in the rangeString
def getElements(rangeString):
    
    elements = []
    
    splitbycomma = rangeString.split(',')
    
    for e in splitbycomma:
        if (e.count('-') > 0) or (e.count('..') > 0):
            #Is a range
            if (e.count('-') > 0):
                erange = e.split('-')
            else:
                erange = e.split('..')
            
            if(len(erange) not in (2,3)) or (int(erange[0]) > int(erange[1])):
                raise Exception('Invalid Range: ' + e) 
            
            step = 1
            if len(erange) == 3:
                step = int(erange[2])
            
            for i in range(int(erange[0]),(1 + int(erange[1])), step):
                elements.append(i)
        else:
            elements.append(int(e))
    
    return elements

# elements is a list of objects. These objects may be duplicated. This method 
# returns a dictionary: the keys are the different elements (without
# duplications). Each value will be a list of the indexes in the original 
# elements array
def getIndexesDictionary(elements):
    
    indexesPerElement = {}
    
    for i in range(len(elements)):
        if elements[i] in indexesPerElement.keys():
            indexes = indexesPerElement[elements[i]]
        else:
            indexes = []
        
        indexes.append(i)
        indexesPerElement[elements[i]] = indexes
        
    return indexesPerElement

# Get the number of elements (duplicated elements are counted as one)
def getNumberOfElements(elements):
    return len(getIndexesDictionary(elements))
            
# Split an array in a list of arrays. The way the partition is done guarantees
# that the sizes of the subarrays may vary +-1
# For example if we have an array with 11 elements and we want 3 subarrays the 
# sizes of them would be 4,4,3 instead of 5,5,1
def splitArray(arrayToSplit, numSubArrays):
    quo = len(arrayToSplit) / numSubArrays
    res = len(arrayToSplit) % numSubArrays
    
    indexInArray = 0
    
    subarrays = []
    for i in range(numSubArrays):
        if i < res:
            numElements=quo+1
        else:
            numElements=quo
        
        subarray = []
        for j in range(numElements):
            subarray.append(arrayToSplit[indexInArray])
            indexInArray = indexInArray + 1
            
        subarrays.append(subarray)
        
    return subarrays

# Split an dictionary in a list of dictionary. The way the partition is done guarantees
# that the sizes of the subdictionary may vary +-1
# For example if we have an dictionary with 11 elements and we want 3 subdictionary the 
# sizes of them would be 4,4,3 instead of 5,5,1
def splitDictionary(dictionaryToSplit, numSubDictionaries):
    quo = len(dictionaryToSplit) / numSubDictionaries
    res = len(dictionaryToSplit) % numSubDictionaries
    
    indexInDictionaryValuesArray = 0
    
    subdictionaries = []
    for i in range(numSubDictionaries):
        if i < res:
            numElements=quo+1
        else:
            numElements=quo
        
        subdictionary = {}
        for j in range(numElements):
            key = dictionaryToSplit.keys()[indexInDictionaryValuesArray]
            subdictionary[key] = dictionaryToSplit[key]
            indexInDictionaryValuesArray = indexInDictionaryValuesArray + 1
            
        subdictionaries.append(subdictionary)
        
    return subdictionaries

def getNextNode(node):
    if node.count('node') > 0:
        index = int(node.replace('node', ''))
        return 'node%03d' % ((index % 80) + 1)
    elif node.count('lce') > 0:
        index = int(node.replace('lce', ''))
        return 'lce%03d' % ((index % 72) + 1)
    elif node.count('lse') > 0: 
        index = int(node.replace('lse', ''))
        return 'lse%03d' % ((index % 24) + 1)
    elif node.count('dop') > 0: 
        index = int(node.replace('dop', ''))
        return 'dop%03d' % ((index % 221) + 1)
    elif node.count('locus') > 0: 
        index = int(node.replace('locus', ''))
        return 'locus%03d' % ((index % 102) + 1)
    else:
        index = int(node)
        return '%03d' % ((index % 80) + 1)

# Get the list of nodes as described by the nodesstring
# Accepted values are node1,2,3,4,6-10 or node15 or lce1,2,5-10/lse1,3,57
def getNodes(nodesstring):
    
    nodes = []
    if nodesstring.count('/') > 0:
        for subnodesstring in nodesstring.split('/'):
            nodes.extend(getNodes(subnodesstring))
    elif nodesstring.count('node') > 0:
        for e in getElements(nodesstring.replace('node', '')):
            nodes.append('node%03d' % e)
    elif nodesstring.count('lofareor') > 0:
        for e in getElements(nodesstring.replace('lofareor', '')):
            nodes.append('lofareor%02d' % e)
    elif nodesstring.count('lce') > 0:
        for e in getElements(nodesstring.replace('lce', '')):
            nodes.append('lce%03d' % e)
    elif nodesstring.count('lse') > 0: 
        for e in getElements(nodesstring.replace('lse', '')):
            nodes.append('lse%03d' % e)
    elif nodesstring.count('dop') > 0: 
        for e in getElements(nodesstring.replace('dop', '')):
            nodes.append('dop%03d' % e)
    elif nodesstring.count('locus') > 0: 
        for e in getElements(nodesstring.replace('locus', '')):
            nodes.append('locus%03d' % e)
    elif nodesstring in REMOTE_STORES: 
        nodes.append(nodesstring)
    else:
        # we consider we are with the nodes of eor cluster if no name is provided
        for e in getElements(nodesstring):
            nodes.append('node%03d' % e)
    return nodes

def getSubBandsToUse(bandsoption):
    if bandsoption == '':
        return ''
    else:
        bands = [] 
        for e in getElements(bandsoption):
            bands.append('SB%03d' % e)
        return bands

# get a time in H,M,S
def getInHMS(seconds):
    hours = seconds / 3600
    seconds -= 3600*hours
    minutes = seconds / 60
    seconds -= 60*minutes
    return "%02d:%02d:%02d" % (hours, minutes, seconds)

def formatPath(pathToFormat):
    if pathToFormat.endswith('/'):
        return pathToFormat[:-1]
    return pathToFormat

def getFloats(rangeString, separation = '-'):
    sps = []
    for s in rangeString.split(separation):
        sps.append(float(s))
    return tuple(sps)

def booleanStringToBoolean(booleanString):
    if booleanString == 'True':
        return True
    elif booleanString == 'False':
        return False
    else:
        raise Exception('Please specify True or False!')
    
def getFloatRepresentationTuple(value):
    if type(value) == float:
        return (float, ("%.6f" % value))
    else:
        return (str, str(value))
    

def getCorrelationName(index):
    if index == 0:
        return 'XX'
    elif index == 1:
        return 'XY'
    elif index == 2:
        return 'YX'
    elif index == 3:
        return 'YY'
    else:
        return None
    
def gdsToPathNode(gdsFile):
    if not os.path.isfile(input):
        raise Exception('Error: ' + input + ' does not exists')
    
    gdsfile = open(input, 'r')
    gdslines = gdsfile.read().split('\n')
    gdsfile.close()
    absPaths = []
    nodes = []
    
    for line in gdslines:
        if line.count('FileName'):
            try:
                absPaths.append(line.split('=')[-1].strip())
            except:
                continue
        elif line.count('FileSys'):
            try:
                nodes.append(line.split('=')[-1].split(':')[0].strip())
            except:
                continue
    
    if len(absPaths) != 0 and len(nodes) == 0:
        cnode = getHostName()
        for i in range(len(absPaths)):
            nodes.append(cnode)
            
    if len(absPaths) != len(nodes):
        raise Exception('Error: reading GDS file')
    
    return (absPaths, nodes)

# Get the number of dimensions of the values
def ndim(values):
    auxObject = values
    keepLoop = True
    ndim = 0
    while keepLoop:
        if type(auxObject) == list:
            auxObject = auxObject[0]
            ndim += 1
        else:
            keepLoop = False
    return ndim

# this look will convert the last dimension tuple in numpy complex
def loopDim(values, ndim, dimension = 1):
    if dimension == ndim-1:
        for i in range(len(values)):
            values[i] = numpy.complex(*values[i])   
    else:
        for i in range(len(values)):
            loopDim(values[i],ndim,dimension+1)
    return values

# Gets a ndim list of float and convert it to ndim-1 complex array
def convertValues(values, dataType = numpy.complex):
    return numpy.ma.array(loopDim(values, ndim(values)),dtype=dataType)

# get the optimal number of rows and columns for a desired number of elements
# We parse a 1-d array into a 2-D matrix.
def getNM(num,rmult):
    sq = math.sqrt(float(num))
    if int(math.ceil(sq)) % 2 == 0:
        nrows = int(math.ceil(sq))
    else:
        nrows = int(math.floor(sq))
    nrows = nrows - (nrows % rmult)
    if nrows < rmult:
        nrows = rmult
    ncols = int(math.ceil(float(num) / nrows))
    return (nrows,ncols)

# get the positions in the optimal 2-d matrix of a certain element (see previous function)
def getPosition(index, nrows, ncols, rmult):
    m = index % ncols
    n = rmult * (index / ncols)
    return (n*(ncols)) + m

def getOwner(ppath):
    return pwd.getpwuid(os.stat(ppath).st_uid)[0]
