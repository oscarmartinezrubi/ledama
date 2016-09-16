#!/usr/bin/env python
"""
gainanim.py

This script is intended to provide a way to generate a movie of all the stations 
gains. The user can choose the animation dimension (by default time)
User can chose whether to plot Real-Imag or Ampl-Phase.
It actually generates the pngs to create the movie and show the command to be
run afterwards (by the user).

Written by Oscar Martinez, Cyril Tasse

version 1.0    23/07/2012: Initial version from original files by Cyril Tasse
        2.0    07/08/2012: Add multiprocessing in several nodes:
                             Data is now split by station. Then each node split
                             each station data per time. Finally, for each time,
                             a node takes all the contributions of the stations
                             and produce an image (all nodes doing this).
        3.0    11/03/2013: Multiprocessing is fully implemented using message passing.
                             Only the data to be plot is read.
        3.1    12/03/2013: Possibility for adding more nodes to create images
        3.2    22/03/2013: Do not crash if some SB have different number of time samples
                           We show message of possible incoherency 
        3.3    25/03/2013: Add delay file        
        3.4    24/04/2013: Add possibility to animate in frequency           
"""
version_string = 'v3.4, 24th April 2013\nWritten by Oscar Martinez, Cyril Tasse'

import os, optparse, multiprocessing, time, sys, re
from multiprocessing import Pipe
from multiprocessing.managers import SyncManager
import numpy as np
import matplotlib
matplotlib.use('Agg')
import pylab
from lofar import parmdb

# Constants
TIME = 'time'
FREQ = 'freq'
DEF_OUTPUT_ANIMATION_NAME = 'animation.mp4'
DEF_OUTPUT_INFO_NAME = 'INFO'
DEFAULT_POLAR_YRANGE = '0.,0.0025,-3.14,3.14'
DEFAULT_CARTESIAN_YRANGE = '-0.0025,0.0025,-0.0025,0.0025'
AUTH_KEY = 'animationdistributionkey'
GAIN_TAB_NAME = 'instrument'
GAIN_TYPE = 'Gain'
AMPL_COORD = 'Ampl'
PHASE_COORD = 'Phase'
REAL_COORD = 'Real'
IMAG_COORD = 'Imag'
COORD_NUM = 2
FIG_SIZE = '12,7'

MESS_TYPE_WORKER_OK_GAIN = 0
MESS_TYPE_WORKER_KO_GAIN = 1
MESS_TYPE_WORKER_SEND_GAIN = 2
MESS_TYPE_WORKER_IMAGE_CREATED = 3
MESS_TYPE_WORKER_END = 4
# MESSAGE FROM CLIENTS
MESS_TYPE_CLIENT_END = 5

def getLDSName(absPath):
    """ Get the LDS name from the MS absPath. 
        In LOFAR EoR cluster the convention is too use as parent path of
        the MSs the following: LXXXX_YYYYY[_ZZZ] where:
            - XXXX is the year where the observation was took [ 4 digits ]
            - YYYYY is the number of the LDS name [at least 5 digits]
              LDS name = LYYYYY
            - ZZZ is the version and it is optional
        If this pattern is not found, we look for something like LYYYYY"""
    f = re.search('L[0-9][0-9][0-9][0-9]_[0-9]*', absPath)
    if f != None:
        return 'L' + (str(f.group(0))).split('_')[-1]
    else:
        f = re.search('L[0-9]*', absPath)
        if f != None:
            return str(f.group(0))
    return None

def getInputDict(inputGDSFilePath, check = True):
    """ From the GDS file we create a dictionary where keys are the nodes 
        (with data) and the values are lists of measurementsets paths in each 
        node """
    ii = open(inputGDSFilePath)
    absPaths = []
    ldss = []
    nodes = []
    nChans = []
    stepTimes = []
    for line in ii:
        if line.count('.FileName'):
            absPaths.append(line.replace('\n','').split('=')[-1].strip())
            ldss.append(getLDSName(absPaths[-1]))
        elif line.count('.FileSys'):
            nodes.append(line.replace('\n','').split('=')[-1].strip().split(':')[0])
        elif line.count('.NChan'):
            nChans.append(line.replace('\n','').split('=')[-1].strip())  
        elif line.count('.StepTime'):
            stepTimes.append(line.replace('\n','').split('=')[-1].strip())    
    if len(absPaths) and check:
        for i in range(len(absPaths)):
            if ldss[i] != ldss[0]:
                raise Exception('ERROR: SBs must be of the same observation')
            elif nChans[i] != nChans[0]:
                raise Exception('ERROR: SBs must have same number of channels')
            elif stepTimes[i] != stepTimes[0]:
                raise Exception('ERROR: SBs must have same step time')
    sbsPerNodeDict = {}
    for i in range(len(absPaths)):
        if nodes[i] not in sbsPerNodeDict:
            sbsPerNodeDict[nodes[i]] = []
        sbsPerNodeDict[nodes[i]].append(absPaths[i])
    return sbsPerNodeDict 

def getJonesElements(jones):
    """ Parse the jones from the args to a list of positions in the Jones matrix
        For example: 0,1 will return [(0, 0), (1, 1)] """
    jonesElements = jones.split(',')
    for i in range(len(jonesElements)):
        jaux = ('0'+(bin(int(jonesElements[i]))[2:]))[-2:]
        jonesElements[i] = (int(jaux[0]),int(jaux[1]))
    return jonesElements
 
def getSlots(slots):
    """ Parse the slots from the args to a list of ints.
        It returns: [initialIndex, endIndex, step] """
    slotssplit = slots.split(',')
    if len(slotssplit) != 3:
        raise Exception('ERROR: slots format is start,end,step')
    for i in range(len(slotssplit)): slotssplit[i] = int(slotssplit[i])
    return slotssplit

def getFigSize(figsize):
    figsize = figsize.split(',')
    if len(figsize) != 2:
        raise Exception('ERROR: figsize format is xsize,ysize')
    for i in range(len(figsize)): figsize[i] = int(figsize[i])
    return tuple(figsize)

def getYRange(yrange, polar):
    """ Parse the yrange from the args to a list of floats 
        (if required, it assigns the default values) """
    if yrange == '':
        if polar:
            yrange = DEFAULT_POLAR_YRANGE
        else:
            yrange = DEFAULT_CARTESIAN_YRANGE
    yrangesplit = yrange.split(',')
    if len(yrangesplit) != 4:
        raise Exception('ERROR: YRange format is min1,max1,min2,max2')
    for i in range(len(yrangesplit)):
        yrangesplit[i] = float(yrangesplit[i])
    return yrangesplit

def getDelayDict(delayFile):
    """ Get a delay dictionary from the given file. It returns all 0 if none 
        file is provided"""
    delayDict = {}
    if delayFile != None:
        lines = open(delayFile,'r').read().split('\n')
        for line in lines:
            if line != '':
                (station,delay) = line.split()[:2]
                delayDict[station] = float(delay)
    return delayDict

def updateDelayDict(delayDict, stations):
    """Update the delay dict with full list of stations"""
    for station in stations:
        if station not in delayDict:
            delayDict[station] = 0.
    for station in delayDict:
        if station not in stations:
            delayDict.pop(station)
    return delayDict

def getHostName():
    """ Get current machine name (required for the ssh and TCP connections) """
    return (os.popen("'hostname'")).read().split('\n')[0]

def getElements(rangeString):
    """ Gets a list of the elements of the rangeString. For example for a 
        rangeString 0,1,5-10,12,14-16 We would get a list as:
        0,1,5,6,7,8,9,10,12,14,15,16
        it return None if we detect any unexpected format in the rangeString"""
    elements = []
    splitbycomma = rangeString.split(',')
    for e in splitbycomma:
        if (e.count('-') > 0) or (e.count('..') > 0):
            if (e.count('-') > 0):
                erange = e.split('-')
            else:
                erange = e.split('..')
            if(len(erange) != 2) or (int(erange[0]) > int(erange[1])):
                print 'Invalid Range: ' + e 
                return None
            for i in range(int(erange[0]),(1 + int(erange[1]))):
                elements.append(i)
        elif e != '':
            elements.append(int(e))
    return elements

def getSBIndex(absPath):
    """ Get the SB index from the MS path"""
    f = re.search('SB[0-9][0-9][0-9]', absPath)
    if f != None:
        return int((str(f.group(0))).replace('SB',''))
    else:
        f = re.search('sb[0-9][0-9][0-9]', absPath)
        if f != None:
            return int((str(f.group(0))).replace('sb',''))
        return None    

def getNodes(nodesstring):
    """ Get the list of nodes as described by the nodesstring
        Accepted values are node1,2,3,4,6-10 or node15 or lce1,2,5-10/lse1,3,57
        Supported common node names are: node, lofareor, lce, lse, dop and locus   
    """
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
    else:
        raise Exception('ERROR: Specified extra nodes could not be parsed. Supported common node names are: node, lofareor, lce, lse, dop and locus')
    return nodes

def log(message):
    """ Method that print to stdout and automatically flushes it"""
    sys.stdout.write(message + '\n')
    sys.stdout.flush()

def logImagesProgress(counter, total, numWorkers):
    """ Show dynamic progress of created images"""
    message = "\rImages completed: %d of %d (%3.1f%%). Num. workers: %4d" % (counter,total, float(counter) * 100./float(total), numWorkers)
    sys.stdout.write(message)
    sys.stdout.flush()

def showMakeMovieCommand(outputFolder, animFile, xaxis):
    """ Shows the commands that the user should execute to create the movie"""
    if xaxis == FREQ:
        rate = 20
    else:
        rate = 8
        print 'Renaming images...'
        os.system('rm -f ' + outputFolder + '/img*png')
        # We need to rename the images to consecutive indexes
        imageIndex = 0
        for imagefile in sorted(os.listdir(outputFolder)):
            if imagefile.endswith('png'):
                imagefilepath = outputFolder + '/' + imagefile
                newimagefilepath = outputFolder  + ('/img%06d.png' % imageIndex)
                os.system('mv ' + imagefilepath + ' ' + newimagefilepath)
                imageIndex += 1

    print 'To generate the movie with mencoder:' 
    print "cd " + outputFolder + "; mencoder -ovc lavc -lavcopts vcodec=mpeg4:vpass=1:vbitrate=4620000:mbd=2:keyint=132:v4mv:vqmin=3:lumi_mask=0.07:dark_mask=0.2:mpeg_quant:scplx_mask=0.1:tcplx_mask=0.1:naq -mf type=png:fps=" + str(rate) + " -nosound -o "+animFile+" mf://\*.png"
    print 
    print 'To generate the movie with ffmpeg:'
    print "ffmpeg -r " + str(rate) + " -i " + outputFolder + "/img%06d.png -vcodec mpeg4 -b:v 4620000 -y " + animFile    
        
def makeInfoFile(infoFilePath, usedData, xaxis, jonesElements, stations, refStation, polar, timeslots, channelsslots, yRange, delayDict, message):
    """ Create a file with information regarding the movie"""
    infoFile = open(infoFilePath, 'w')
    infoFile.write('XAXIS: ' + str(xaxis) + '\n')
    infoFile.write('JONES: ' + str(jonesElements) + '\n')
    infoFile.write('STATIONS: ' + ','.join(stations) + '\n')
    delays = []
    for station in stations:
        delays.append(str(delayDict[station]))
    infoFile.write('DELAYS: ' + ','.join(delays) + '\n')
    infoFile.write('REFSTATION: ' + str(refStation) + '\n')
    infoFile.write('POLAR: ' + str(polar) + '\n')
    infoFile.write('TIMES: ' + str(timeslots) + '\n')
    infoFile.write('CHANNELS: ' + str(channelsslots) + '\n')
    infoFile.write('YRANGE: ' + str(yRange) + '\n')
    infoFile.write('MESSAGE: ' + str(message) + '\n')
    infoFile.write('USEDDATA:\n')
    for (node, msPath) in sorted(usedData):
        infoFile.write(node + ' ' + msPath + '\n')
    infoFile.close()

def getGainStation(db, polarInDB, polarUser, station, Nt, tIndexInit, tIndexEnd, tIndexStep, Nc, cIndexInit,cIndexEnd,cIndexStep, corrList, coordList):
    """ Gets the gain solutions from the parmdb for requested station, 
        the time samples (specified by Nt, tIndexInit, tIndexEnd and tIndexStep)
        and the elements in the jones matrix (corrList).
        The way the data is stored in the parmdb (polar or cartessian) must also
        be specified and also the list of detected coordinates 
        (Real/Imag or Ampl/Phase)"""
    numcorr = len(corrList)
    # Initialize the Gain 2D matrix. The initialization depends on whether the 
    # data is in polar coordinates or not
    if polarInDB:
        Gains = np.ones((Nt,Nc,numcorr),dtype=np.complex64)
    else:
        Gains = np.zeros((Nt,Nc,numcorr),dtype=np.complex64)
    for i in range(numcorr):
        # for each requested element in the jones matrix
        (p0,p1) = corrList[i]
        for coord in coordList:
            name = ':'.join((GAIN_TYPE,str(p0),str(p1),coord,station))
            # NOTE: we only get first channel
            values = db.getCoeff(name)[name]['values'][tIndexInit:tIndexEnd:tIndexStep,cIndexInit:cIndexEnd:cIndexStep]
            # Depending on the coordinate we add the information in one way or another
            if coord == REAL_COORD :
                Gains[:,:,i]+= values
            elif coord == IMAG_COORD:
                Gains[:,:,i]+=(1j) * values
            elif coord == AMPL_COORD:
                Gains[:,:,i]*=values
            else: # coord == PHASE_COORD:
                Gains[:,:,i]*= np.cos(values) + (1j*np.sin(values))
    # We return a tuple of two 2 4D (time,freq,jones element). Depending on what 
    # the use requested we return Amp and Phase or Real and Imag
    if polarUser:
        return (np.abs(Gains),np.angle(Gains))
    else:
        return (np.real(Gains),np.imag(Gains))

def getGain(absPath, stationsPattern, jonesElements, polarUser, tIndexInit, tIndexEnd, tIndexStep, cIndexInit,cIndexEnd,cIndexStep):
    """Uses parmdb to extract all the gain solutions for the instrument table of
       the measurement set specified by absPath. Which stations are requested is
       specified by stationsPattern. Only the elements in the Jones matrix 
       specified by jonesElements are requested. Only times specified by 
       tIndexInit, tIndexEnd and tIndexStep are used"""
    gainTableAbsPath = absPath + '/' + GAIN_TAB_NAME
    try:
        if os.path.isdir(gainTableAbsPath):
            # Open the parmdb (locally)
            db = parmdb.parmdb(gainTableAbsPath)
            namesList=db.getNames('*:%s'%(stationsPattern))
            # We iterate over the names to find different stations, correlations and 
            # complex coordinates (we apply the station pattern)
            stationSet = set([])
            corrSet = set([])
            cordSet = set([])
            for name in namesList:
                g,p0,p1,cord,station=name.split(":")
                if g != GAIN_TYPE:
                    raise Exception('ERROR: This tool only works for Gain instrument tables')
                # Add the station and correlation to its sets
                corrSet.add((int(p0),int(p1)))
                stationSet.add(station)
                cordSet.add(cord)  
                
            # We get the lists and sort them
            stationList = sorted(list(stationSet))
            corrList = sorted(list(corrSet))
            cordList = sorted(list(cordSet))
            
            # Check the complex coordinates
            if AMPL_COORD in cordList and PHASE_COORD in cordList:
                polarInDB = True
            elif REAL_COORD in cordList and IMAG_COORD in cordList:
                polarInDB = False
            else:
                raise Exception('ERROR: paramdb contains ' + ','.join(cordSet))
            
            # Check that the demanded elements in the jones matrix are available
            for jonesElement in jonesElements:
                if jonesElement not in corrList:
                    raise Exception('ERROR: Jones element ' + str(jonesElement) + ' not found.')
            
            # Get and check number of stations
            numStations = len(stationList)
            if not numStations:
                raise Exception('ERROR: None station found with specified pattern')
            
            # We assume all the parameters have the same times and freqs than the first one
            nameRef=namesList[0]
            allTimes = db.getCoeff(nameRef)[nameRef]['times']
            numAllTimes = allTimes.shape[0]
            if tIndexEnd == -1 or tIndexEnd > numAllTimes:
                tIndexEnd = numAllTimes
            else:
                tIndexEnd += 1
            # We get the array of times requested by the user
            times = allTimes[tIndexInit:tIndexEnd:tIndexStep] 
            # Number of times reuested by user
            Nt=times.shape[0]
            # We get the frequency of current SB
            allFreqs = db.getCoeff(nameRef)[nameRef]['freqs']
            numAllFreqs = allFreqs.shape[0]
            if cIndexEnd == -1 or cIndexEnd > numAllFreqs:
                cIndexEnd = numAllFreqs
            else:
                cIndexEnd += 1
            # We get the array of times requested by the user
            freqs = allFreqs[cIndexInit:cIndexEnd:cIndexStep] 
            # Number of times reuested by user
            Nc=freqs.shape[0]
            
            # We iterate and get all the data for each station
            gains = [] 
            for i in range(numStations):
                gains.append(getGainStation(db, polarInDB, polarUser, stationList[i], Nt, tIndexInit, tIndexEnd, tIndexStep, Nc, cIndexInit,cIndexEnd,cIndexStep, jonesElements, cordList)) 
            # Close the paramdb
            db = 0
            # gains has dimensions: station, cord, time, freq, jones
            gains = np.array(gains).transpose((3,2,0,1,4))
            # new dimensions freq, time, station, cord, jones
            return (stationList, gains, allTimes[0], times, freqs)
    except Exception,e:
        log('ERROR: loading gain from ' + gainTableAbsPath + ': ' + str(e))
    # If none instrument table is found or any error has happened we return al Nones
    return (None,None,None,None,None)

def getPlotArgs(numStations, yRange):
    """ Get plotting fields arguments of the pictures layout"""
    ny=int(np.sqrt(numStations/1.77))
    nx=int(numStations/ny) 
    if nx*ny < numStations:
        nx+=1
    xcoord=np.linspace(0.05,.95,nx+1)
    ycoord=np.linspace(0.05,.95,ny+1)
    acoord=np.zeros((numStations,2),dtype=np.float)
    for ant in range(numStations)[::-1]:
        i=int(ant/nx)
        j=ant-i*nx
        acoord[ant,0]=xcoord[j]
        acoord[ant,1]=ycoord[i]
    dx=xcoord[1]-xcoord[0]
    dy=(ycoord[1]-ycoord[0])/2.
    margx=4e-3
    margy=2.e-2
    ylim= [yRange[0:2],yRange[2:4]]
    return (acoord, margx, margy, ylim, dx, dy)

def setLabels(axis, polar, antIndex, cordIndex, xLabel):
    """ Set the labels to the pylab axis"""
    # we add the lables (only for first ant which will be in bottom-left)
    if antIndex==0:
        if cordIndex == 0:
            axis.set_xlabel(xLabel)
            if polar:
                axis.set_ylabel("Amp")
            else:
                axis.set_ylabel("Real")
        else:
            if polar:
                axis.set_ylabel("Pha")
            else:
                axis.set_ylabel("Imag")

def runPlotWorker(workerName, sharedJobQueue, sharedResultQueue, stations, outputFolder, itime, times, polar, refStationIndex, acoord, margx, margy, ylim, dx, dy, delay, figSize):
    """ Plot-Worker that is executed in remote clients when xaxis==freq.
        They have an input queue where to get the data to plot.
        And, also a output queue where to indicate what they have done.
        The images are stored in the outputFolder (which should be a directory 
        shared by all the involved machines)"""
    # Get number of stations and the refStation name
    numStations = len(stations)
    refStation = stations[refStationIndex]
    # Initialize the min and max freqs (in the job we will assign their values)
    (minFreq,maxFreq) = (None, None)
    # Initialize the figure
    fig = pylab.figure(figsize=figSize)
    # Initialize the variable that will indicate when the killing-job is received
    kill_received = False
    while not kill_received:
        job = None
        try:
            job = sharedJobQueue.get() # wait until new job is available
            if job == None:
                # If we receive a None job, it means we can stop this worker 
                kill_received = True
                log(workerName + ' kill received!')
            else:
                ti, tgains, freqs = job # tgains dimensions are time, station, cord, jones, freq
                if minFreq == None:
                    (minFreq,maxFreq) = (float(np.min(freqs)/1e6),float(np.max(freqs)/1e6))
                # For all the times indexes (relative to ti) we create images
                for tir in range(len(tgains)):
                    # Clean the figure
                    pylab.clf()
                    fig.clf()
                    imageName = outputFolder + ('/img%06d.png' % (ti+tir))
                    Jones2Ref = None
                    if polar:
                        Jones2Ref = tgains[tir,refStationIndex,1]
                    for antIndex in range(numStations):
                        stationGains = tgains[tir,antIndex]
                        if polar:
                            stationGains[1] -= Jones2Ref
                            stationGains[1] -= 2 * np.pi * freqs * 1e6 * delay[stations[antIndex]]
                            stationGains[1] = np.remainder(stationGains[1],2*np.pi) - np.pi
                        a = None
                        addposy = [margy, dy]
                        # for (Real and Imag) or (Ampl and Phase)
                        for cordIndex in range(COORD_NUM):
                            # we create a sub-plot
                            a = fig.add_axes([acoord[antIndex,0]+margx, acoord[antIndex,1]+addposy[cordIndex], dx-2.*margx,dy-margy])
                            # for each element in the jones matrix (that we selected to plot)
                            for j in range(len(stationGains[cordIndex])):
                                a.plot(freqs,stationGains[cordIndex,j,:],marker='.',ls='',mew=0.1,ms=1.5,color='black')
                            pylab.setp(a, xticks=[], yticks=[],ylim=ylim[cordIndex])
                            setLabels(a, polar, antIndex, cordIndex, "Freq")
                        a.title.set_text(stations[antIndex])
                        a.title.set_fontsize(9)
                    # Set general title
                    if polar:
                        imageTitle="t=%.1f, Freqs=[%5.1f, %5.1f]MHz, RefStation=%s"%(times[(ti+tir)]-itime,minFreq,maxFreq,refStation)
                    else:
                        imageTitle="t=%.1f, Freqs=[%5.1f, %5.1f]MHz"%(times[(ti+tir)]-itime,minFreq,maxFreq)
                    a.annotate(imageTitle, xy=(0.5, 0.97),  xycoords='figure fraction', horizontalalignment="center")
                    # Creat the image
                    fig.savefig(imageName)
                    log(workerName + ' ' + imageName + ' created')
                    # Put in the output queue that we created a new image
                    sharedResultQueue.put([MESS_TYPE_WORKER_IMAGE_CREATED,]) 
        except:
            # if there is an error we will quit the generation of pictures
            kill_received = True
    sharedResultQueue.put([MESS_TYPE_WORKER_END,])
    log(workerName + ' Exiting...')

def runReadWorker(workerName, node, recvJobPipeRW, sharedResultQueue, msPath, stationsPattern, jonesElements, polar, tIndexInit, tIndexEnd,tIndexStep, cIndexInit,cIndexEnd,cIndexStep, refStation, chunkSize):
    """ Read Worker that is executed in remote clients when xaxis=freq. 
        First it loads its realted SB and then, under request, it sends the 
        chunks of data to the main server
        """
    try:
        # Loads the gains (dimensions are freq, time, station, cord, jones)
        (stations, gains, itime, times, freqs) = getGain(msPath, stationsPattern, jonesElements, polar, tIndexInit, tIndexEnd,tIndexStep, cIndexInit,cIndexEnd,cIndexStep )
        # Check if data was successfully loaded
        if gains == None:
            message = workerName + ' could not load gains from ' + str(msPath) 
            log(message)
            sharedResultQueue.put([MESS_TYPE_WORKER_KO_GAIN, message,],)
        else:
            refStationIndex = 0
            if refStation != '' and refStation in stations:
                refStationIndex = stations.index(refStation)    
            message = workerName + ' reading gains from ' + str(msPath) + (' (%5.1f MHz' % float(freqs[0]/1e6)) + ')'
            log(message) 
            # Send message that we are done reading the gains, so we are ready to starting 
            # the plotters and the queries for time chunks
            sharedResultQueue.put([MESS_TYPE_WORKER_OK_GAIN, message, node, msPath, itime, times, stations, refStationIndex])
        # Next part is not executed until server has info from all SBs
        continueRecv = True
        while continueRecv:
            data = recvJobPipeRW.recv() # we recieve message from the main client process that the server is asking for cerain chunk
            initTimeIndex = data['TIMEINDEX']
            if (gains != None) and (initTimeIndex >= 0):
                endTimeIndex = initTimeIndex + chunkSize
                if endTimeIndex>=len(times):
                    endTimeIndex = len(times)
                # we get the gains solutions fore the requested time chunk
                gainsToSend = gains[:,initTimeIndex:endTimeIndex]
                log(workerName + ' sending data for t=[' + str(initTimeIndex) + ',' + str(endTimeIndex) + ') ALL: ' + str(gains.shape) + ', SEND: ' + str(gainsToSend.shape))
                sharedResultQueue.put([MESS_TYPE_WORKER_SEND_GAIN,gainsToSend, freqs],)
            else:
                continueRecv = False
    except Exception,e:
        log(workerName + ' ERROR: ' + str(e))
        pass
    sharedResultQueue.put([MESS_TYPE_WORKER_END,])
    log(workerName + ' exiting...')

def runWorker(workerName, node, clientJobQueue, sharedResultQueue, stationsPattern, jonesElements, polar, tIndexInit, tIndexEnd, tIndexStep, cIndexInit,cIndexEnd,cIndexStep, refStation, yRange, delayDict, outputFolder, figSize):
    """ Worker that is executed in the remote clients when xaxis==time. 
        They get from the queue the SB, and for each one they create their 
        related images"""
    kill_received = False
    firstSB = True
    (minTime,maxTime) = (None, None)
    while not kill_received:
        job = None
        try:
            job = clientJobQueue.get() # wait until new job is available
            if job == None:
                # If we receive a None job, it means we can stop this worker
                kill_received = True
                log(workerName + ' kill received!')
            else:
                [msPath,] = job
                # Loads the gains (freq, time, station, cord, jones)
                (stations, gains, itime, times, freqs) = getGain(msPath, stationsPattern, jonesElements, polar, tIndexInit, tIndexEnd, tIndexStep, cIndexInit, cIndexEnd, cIndexStep)
                if stations == None:
                    message = workerName + ' could not load gains from ' + str(msPath) 
                    log(message)
                    sharedResultQueue.put([MESS_TYPE_WORKER_KO_GAIN, message,],)
                else:
                    if firstSB: # We only do this checking for first SB
                        firstSB = False
                        refStationIndex = 0
                        if refStation != '' and refStation in stations:
                            refStationIndex = stations.index(refStation)    
                        refStation = stations[refStationIndex]
                        delayDict = updateDelayDict(delayDict, stations)
                        (minTime,maxTime) = (times[0] - itime,times[-1] - itime) 
                    
                    # Initialize the figure anf get layout parameters
                    log(workerName + ' plotting gains from ' + str(msPath) + (' (%5.1f MHz' % float(freqs[0]/1e6)) + ')') 
                    (acoord, margx, margy, ylim, dx, dy) = getPlotArgs(len(stations), yRange)
                    fig = pylab.figure(figsize=figSize)
                    sbIndex = getSBIndex(msPath)
                    # For all the channel indexes we create images
                    for ci in range(len(gains)):
                        # Clean the figure
                        pylab.clf()
                        fig.clf()
                        imageName = outputFolder + ('/timg%03d%03d.png' % (sbIndex,ci))
                        Jones2Ref = None
                        if polar:
                            Jones2Ref = gains[ci,:,refStationIndex,1] 
                        for antIndex in range(len(stations)):
                            stationGains = gains[ci,:,antIndex]
                            if polar:
                                stationGains[:,1] -= Jones2Ref
                                stationGains[:,1] -= 2 * np.pi * freqs[ci] * 1e6 * delayDict[stations[antIndex]]
                                stationGains[:,1] = np.remainder(stationGains[:,1],2*np.pi) - np.pi
                            a = None
                            addposy = [margy, dy]
                            # for (Real and Imag) or (Ampl and Phase)
                            for cordIndex in range(COORD_NUM):
                                # we create a sub-plot
                                a = fig.add_axes([acoord[antIndex,0]+margx, acoord[antIndex,1]+addposy[cordIndex], dx-2.*margx,dy-margy])
                                # for each element in the jones matrix (that we selected to plot)
                                for j in range(len(stationGains[0][cordIndex])):
                                    a.plot(times,stationGains[:,cordIndex,j],marker='.',ls='',mew=0.1,ms=1.5,color='black')
                                pylab.setp(a, xticks=[], yticks=[],ylim=ylim[cordIndex])
                                setLabels(a, polar, antIndex, cordIndex, "Time")
                            a.title.set_text(stations[antIndex])
                            a.title.set_fontsize(9)
                        # Set general title
                        if polar:
                            imageTitle="f=%.1fMHz, Times=[%5.1f, %5.1f]s, RefStation=%s"%(freqs[ci]/1e6,minTime,maxTime,refStation)
                        else:
                            imageTitle="f=%.1fMHz, Times=[%5.1f, %5.1f]s"%(freqs[ci]/1e6,minTime,maxTime)
                        a.annotate(imageTitle, xy=(0.5, 0.97),  xycoords='figure fraction', horizontalalignment="center")
                        # Save the image
                        fig.savefig(imageName)
                        log(workerName + ' ' + imageName + ' created')
                        # Put in the output queue that we created a new image
                        sharedResultQueue.put([MESS_TYPE_WORKER_IMAGE_CREATED,]) 
                    # Send message to indicate we are done with this SB
                    sharedResultQueue.put([MESS_TYPE_WORKER_OK_GAIN, node, msPath, freqs, stations, refStationIndex,])
        except:
            # if there is an error we will quit the generation
            kill_received = True
    sharedResultQueue.put([MESS_TYPE_WORKER_END,])
    log(workerName + ' exiting...')
                
def runClient(snode, port):
    """ The function which is run in each remote client. 
        First of all, from the recv_job_p pipe the client receives the info on 
        which (and how) gains solutions (measurement set) to read.
        Then, depending on xaxis:
         1 - if xaxis == time, we create processes in charge of both reading 
         and plotting. When they are done, they will the server know and the 
         server will send message to main client process indicating that it can
         terminate its execution.
         2 - if xaxis == freqs, we create other processes (read-workers) 
         in charge of accessing the data. There are as many read-workers as SBs 
         in this node. Then these new read-workers load in local memory the 
         gains solutions and let the server know that they are ready to start 
         further processing. Then, the main client process will create other 
         processes in charge of creation of images (plot-workers). Third, it 
         remains listening to the recv_job_p where the server will query for 
         chunks of the loaded gains and forward these info to read-workers. 
         When all the gains solutions have been sent (under server request) and 
         all the plot-workers have finished their plotting tasks the life of 
         this main process is over
    """
    manager = makeClientManager(snode, int(port), AUTH_KEY)
    node = getHostName()
    print 'Getting job receiving pipe'
    recvJobPipe = manager.get_job_p_r(node)
    print 'Getting queues'
    sharedResultQueue = manager.get_result_q()
    
    # First data we receive is the info
    print 'Getting initial data...'
    data = recvJobPipe.recv()
    outputFolder = data['OUTPUT']
    mspaths = data['PATHS']
    numWorkers = data['NUMWORKERS']
    xaxis = data['XAXIS']
    jonesElements = data['JONES']
    stationsPattern = data['STATIONS']
    delayDict = data['DELAYS']
    refStation = data['REFSTATION']
    polar = data['POLAR']
    timeslots = data['TIMES']
    channelsslots = data['CHANNELS']
    yRange = data['YRANGE']
    chunkSize = data['TIMECHUNKSIZE']
    figSize = data['FIGSIZE']
    tIndexInit,tIndexEnd,tIndexStep = timeslots
    cIndexInit,cIndexEnd,cIndexStep = channelsslots
    
    workers = []
    workersNames = []
    if xaxis == TIME:
        # Create queue in client node to comunicate with workers
        clientJobQueue = multiprocessing.Queue()
        for mspath in mspaths:
            clientJobQueue.put([mspath,])
        for i in range(numWorkers):
            workersNames.append(node + ('-worker%02d' % i))
            clientJobQueue.put(None) # we put a None in the job for each worker (they are used to indicate them to finish)
            workers.append(multiprocessing.Process(target=runWorker, 
                args=(workersNames[-1], node, clientJobQueue, sharedResultQueue, stationsPattern, jonesElements, polar, 
                      tIndexInit, tIndexEnd, tIndexStep, cIndexInit, cIndexEnd, cIndexStep, 
                      refStation, yRange, delayDict, outputFolder, figSize)))
            workers[-1].start()
    else: # xaxis == FREQ
        sharedJobQueue = manager.get_job_q()
        numReadWorkers = len(mspaths)
        numPlotWorkers = numWorkers
        log('Starting ' + str(numReadWorkers) + ' read-workers')
        sendJobPipeRWs = []
        for i in range(numReadWorkers):
            workersNames.append(node + ('-readWorker%02d' % i))
            # Create pipe to communicate the main client process with the several read-workers
            recvJobPipeRW, sendJobPipeRW = Pipe(False)
            sendJobPipeRWs.append(sendJobPipeRW)
            workers.append(multiprocessing.Process(target=runReadWorker, 
                args=(workersNames[-1], node, recvJobPipeRW, sharedResultQueue, mspaths[i], 
                      stationsPattern, jonesElements, polar, tIndexInit, tIndexEnd, tIndexStep, 
                      cIndexInit,cIndexEnd,cIndexStep, refStation, chunkSize)))
            workers[-1].start()
            
        # Once server has acknoledge of initialization of all SBs in all nodes
        # we will get a message with indications 
        data = recvJobPipe.recv()
        stations = data['STATIONS']
        itime = data['ITIME']
        times = data['TIMES']
        refStationIndex = data['REFSTATION']
        delay = data['DELAYS']
        log('Starting ' + str(numPlotWorkers) + ' plot workers')
        # Compute the variable needed for plots
        (acoord, margx, margy, ylim, dx, dy) = getPlotArgs(len(stations), yRange)
        for i in range(numPlotWorkers):
            workersNames.append(node + ('-plotWorker%02d' % i))
            workers.append(multiprocessing.Process(target=runPlotWorker,
                    args=(workersNames[-1], sharedJobQueue, sharedResultQueue, stations, outputFolder, 
                          itime, times, polar, refStationIndex, acoord, margx, margy, ylim, dx, dy, delay, figSize)))
            workers[-1].start()
        
        continueRecv = True
        while continueRecv:
            data = recvJobPipe.recv() # this message indicates a new chunk is requested
            if data['TIMEINDEX'] < 0: 
                # this means the server does not want more chunks, we can exit 
                # (after telling so to the read-workers)!
                continueRecv = False
            for i in range(numReadWorkers):
                sendJobPipeRWs[i].send(data)
        # In this point, we can close these ends of the pipes
        for i in range(numReadWorkers):
            sendJobPipeRWs[i].close()
        
    # We wait until all the workers are done
    for i in range(len(workers)): # we do not use numWorkers because in case of xaxis == FREQ there are also the readWorkers
        workers[i].join()
        log(workersNames[i] + ' joined!')
    log('All workers exited!')
    # Indicate that all workers are done
    sharedResultQueue.put([MESS_TYPE_CLIENT_END,],)
    return

def makeClientManager(server, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address:port and exposes the get_job_p_r, get_job_q and 
        get_result_q methods for accessing the pipes and shared queue from the 
        server. Return a manager object.
    """
    class JobManager(SyncManager): pass
    JobManager.register('get_job_p_r')
    #JobManager.register('get_job_p_s') #NOT REQUIRED
    JobManager.register('get_job_q') # this will only be used if xaxis==freq
    JobManager.register('get_result_q')
    manager = JobManager(address=(server, port), authkey=authkey)
    manager.connect()
    log('Client connected to %s:%s' % (server, port))
    return manager

def runRemoteClient(node, snode, port, outputFolder):
    """ Function to make an ssh to run the client code in a remote machine 
        We assume the node is reachable via ssh (and the internal network is 
        properly configured). We also assume that in the remote machine the current 
        script file is in the same location that in current machine.
    """
    scriptpath  = os.path.abspath(__file__)
    parentpath = os.path.abspath(os.path.join(scriptpath, '..'))
    scriptname = scriptpath.split('/')[-1].split('.')[0]
    command = 'python -c "import ' + scriptname + '; ' + scriptname + '.' + runClient.__name__ + '(\\\"' + snode +'\\\", \\\"' + str(port) +'\\\")"'
    logging = (' > ' + outputFolder + '/' + node + '.log')
    return (os.popen("ssh " + node + " 'cd " + parentpath + " ; " + command + "'" + logging)).read()

def runServer(manager, outputFolder, xaxis, jonesElements, stationsPattern, refStation, polar, timeslots, channelsslots, yRange, numWorkers, chunkInSize, chunkOutSize, sbsPerNodeDict, delayDict, figSize):
    """ The server which is run in current machine. From the manager we get the 
        involved pipes and queues. We send to the remote clients a message to 
        processing the gain solutions
        Then, depending on xaxis:
        1- For xaxis==time each client will create workers (as many as numWorkers).
          Each worker will process a SB (or more) and for each SB create its 
          related images. We do not require further communication in this case
        2- For xaxis==freq we need communication with central server to join, 
         for each time sample, all the solutions from different SBs (and nodes).
         So, each remote client will create as many read-workers as SBs in the node.
         Then we split the times in chunks and request the gains solutions to all 
         the read-wrokers for each chunk. When we have all the gains (for a given chunk) 
         for all SBs, we summit to the job queue all the tasks for the plot workers
    """
    numClients = len(sbsPerNodeDict)
    finishClientCounter = 0
    
    stations = []
    imagesCounter = 0
    usedData = []
    message = ''
    
    finishWorkersCounter = 0
    numTotalWorkers = None
    
    numSBs = 0
    sendJobsPipes = {}
    for node in sbsPerNodeDict:
        sendJobsPipes[node] = manager.get_job_p_s(node)
        numSBs += len(sbsPerNodeDict[node])
        data = {}
        data['OUTPUT'] = outputFolder
        data['PATHS'] = sbsPerNodeDict[node]
        data['NUMWORKERS'] = numWorkers
        data['XAXIS'] = xaxis
        data['JONES'] = jonesElements
        data['STATIONS'] = stationsPattern
        data['REFSTATION'] = refStation
        data['DELAYS'] = delayDict
        data['POLAR'] = polar
        data['TIMES'] = timeslots
        data['CHANNELS'] = channelsslots
        data['YRANGE'] = yRange
        data['TIMECHUNKSIZE'] = chunkInSize
        data['FIGSIZE'] = figSize
        sendJobsPipes[node].send(data) # We send the messages with details to all the clients
        
    # The queue to receive gains and message from remote clients (from their read-workers more concretely)
    sharedResultQueue = manager.get_result_q()
    
    if xaxis == TIME:
        # We can close the pipes since in this case we do not need them anymore
        for node in sbsPerNodeDict:
            sendJobsPipes[node].close()
        
        # In this case, the clients have already created the workers and started
        # creating the images (we do not require communication with server)
        errorMessages = []
        numChannelsDict = {}
        numTotalWorkers = numWorkers * numClients
        freqs = [None,]
        while finishWorkersCounter < numTotalWorkers:
            # Gets the results from the result queue. there can be several types of messages
            rmess = sharedResultQueue.get()
            rmessType = rmess[0]
            if rmessType == MESS_TYPE_WORKER_END:
                finishWorkersCounter += 1
            elif rmessType == MESS_TYPE_WORKER_KO_GAIN:
                [emessage, ] = rmess[1:]
                errorMessages.append(emessage)
            elif rmessType == MESS_TYPE_WORKER_OK_GAIN:
                [node, absPath, freqs, stations, refStationIndex] = rmess[1:]
                if absPath not in numChannelsDict:
                    # First time we recieve data from certain SB
                    numChannelsDict[absPath] = len(freqs)
                    usedData.append((node, absPath))
            elif rmessType == MESS_TYPE_WORKER_IMAGE_CREATED:
                imagesCounter += 1 
            elif rmessType == MESS_TYPE_CLIENT_END: # Clients where all teh workers finished
                finishClientCounter += 1
            logImagesProgress(imagesCounter, numSBs * len(freqs), numTotalWorkers - finishWorkersCounter)
        print # we print blank line after the dynamic writting
        # Show possible accumulated error messages
        if len(errorMessages):
            print str(len(errorMessages)) + ' SBs had errors loading Gains:'
            for errorMessage in errorMessages:
                print ' ' + errorMessage
        
        # All workers finished loading and plotting data. Waiting clients finalization
        while (finishClientCounter < numClients):
            try:
                sharedResultQueue.get() # They are all MESS_TYPE_END_WORKERS messages
                finishClientCounter += 1
            except:
                pass
        
        if len(numChannelsDict): # check that all SBs had the same number of channels
            numChannels = numChannelsDict[numChannelsDict.keys()[0]]
            for absPath in numChannelsDict:
                if numChannelsDict[absPath] != numChannels:
                    message = 'WARNING: GAINS MAY NOT BE COHERENT (different SBs having different number of channels)'
                    print message
                    break
        # update the delayDict with the full list of stations
        delayDict = updateDelayDict(delayDict, stations)
    else: # xaxis==FREQ
        # the queue to send jobs for the plot-workers
        sharedJobQueue = manager.get_job_q()
        # Wait until all messages from read-workers (one for each SB) are back 
        times = []
        numReadWorkers = numSBs
        numTotalWorkers = numReadWorkers + (numWorkers * numClients)
        readWorkersCounter = 0
        while readWorkersCounter < numReadWorkers:
            rmess = sharedResultQueue.get()
            rmessType = rmess[0]
            if rmessType == MESS_TYPE_WORKER_KO_GAIN:
                [emessage, ] = rmess[1:]
                print emessage
            elif rmessType == MESS_TYPE_WORKER_OK_GAIN:
                [omessage, node, absPath, itime, times, stations, refStationIndex] = rmess[1:]
                usedData.append((node, absPath))
                print omessage
            elif rmessType == MESS_TYPE_WORKER_END:
                finishWorkersCounter += 1
            readWorkersCounter += 1
        numCorrectSBs = len(usedData) # There will be as many active read-workers as SBs with Gains
        numTimes = len(times)
        log('All clients (read-workers) finished loading data. Starting querying chunks and creating images. Num. freqs (read-workers) = ' + str(numCorrectSBs) + ', Num. stations = ' + str(len(stations)) + ', Num. times = ' + str(numTimes) + '...')
        # update the delayDict with the full list of stations
        delayDict = updateDelayDict(delayDict, stations)
        
        # Send to the clients the details of the readed data
        data = {}
        data['STATIONS'] = stations
        data['DELAYS'] = delayDict
        data['ITIME'] = itime
        data['TIMES'] = times
        data['REFSTATION'] = refStationIndex
        for node in sbsPerNodeDict:
            sendJobsPipes[node].send(data)
        
        # In this point all read-workers have finished reading data and are ready to start
        # receiving queries for chunks of data, as well as the plot-workers to plot
        numImages = 0
        for ti in range(0,numTimes,chunkInSize):
            # for each chunk of times
            receivedGain = 0
            sdata = {'TIMEINDEX':ti}
            chunkfreqs = []
            chunkgains = []
            # send all the messages to query for this chunk of times
            for node in sbsPerNodeDict:
                sendJobsPipes[node].send(sdata)
            # we wait until we receive all the gains from all the clients 
            while receivedGain < numCorrectSBs:
                rmess = sharedResultQueue.get()
                rmessType = rmess[0]
                if rmessType in (MESS_TYPE_WORKER_IMAGE_CREATED,MESS_TYPE_CLIENT_END):
                    if rmessType == MESS_TYPE_WORKER_IMAGE_CREATED:
                        # in the meanwhile it may happend that we receive messages of finished images
                        imagesCounter += 1
                    else: # rmessType == MESS_TYPE_CLIENT_END:
                        # in the meanwhile it may happen that we receive messaged of
                        # clients that have finished their tasks (readers and plotters)
                        finishClientCounter += 1
                    logImagesProgress(imagesCounter, numTimes, numWorkers * (numClients - finishClientCounter))
                elif rmessType == MESS_TYPE_WORKER_END:
                    finishWorkersCounter += 1 
                elif rmessType == MESS_TYPE_WORKER_SEND_GAIN:
                    receivedGain += 1
                    [fgains, freqs] = rmess[1:] #fgains dim freq, time, station, cord, jones
                    chunkgains.extend(fgains)
                    chunkfreqs.extend(freqs)
            # we have received all the clients contributions for this chunk
            chunkgains = np.array(chunkgains) #dimensions are: freq, time, station, cord, jones
            chunkfreqs = np.array(chunkfreqs)
            if len(chunkgains.shape) == 5:
                # if shape is not 5, it means that probably some gains have number of time samples
                # all gains must be dimensioned to: time, station, cord, jones, freq
                allfgains = chunkgains.transpose((1,2,3,4,0)) 
                # For the received chunk we create other smallet chunks of times. 
                # Those are send to the queue to be plotted
                for tic in range(0, len(allfgains), chunkOutSize):
                    numImages += 1
                    sharedJobQueue.put([tic+ti, allfgains[tic:tic+chunkOutSize], chunkfreqs])
    
        # We send negative index to indicate to the readers that they can finish
        sdata = {'TIMEINDEX':-1}
        for node in sbsPerNodeDict:
            sendJobsPipes[node].send(sdata)
        # in this point we are done reading all the gains from remote clients
        # We can close the pipes since we already have all of them ready
        for node in sbsPerNodeDict:
            sendJobsPipes[node].close()
        # We add in the end of the queue as many None as numworkers, this is the 
        # ending task of each plot-worker and it is used to tell them theirs tasks are done!
        for node in sbsPerNodeDict:
            for i in range(numWorkers):
                sharedJobQueue.put(None)
        # Wait all the workers to finish their tasks
        while finishClientCounter < numClients:
            try:
                rmess = sharedResultQueue.get()
                rmessType = rmess[0]
                if rmessType == MESS_TYPE_WORKER_IMAGE_CREATED:
                    imagesCounter += 1
                elif rmessType == MESS_TYPE_WORKER_END:
                    finishWorkersCounter += 1 
                elif rmessType == MESS_TYPE_CLIENT_END:
                    finishClientCounter += 1
                logImagesProgress(imagesCounter, numImages, numWorkers * (numClients - finishClientCounter))
            except:
                pass
        print # we print blank line after the dynamic writting
        if imagesCounter < numImages: # theoretically if all clients are done, all the images should have been generated
            print 'ERROR: there are still images in the queue (' + (numImages-imagesCounter) + ') but all clients are finished!'
        if finishWorkersCounter < numTotalWorkers: # theoretically if all cleints are donem all workers should be done
            print 'ERROR: there are alive workers (' + (numTotalWorkers-finishWorkersCounter) +  ') but all clients are finished!'
        if numImages != numTimes:
            message = 'WARNING: GAINS MAY NOT BE COHERENT (different SBs having different number of time samples). Num. Times = ' + str(numTimes) + '. Images generated: ' + str(numImages) 
            print message
    # Independently of xaxis, in both cases we must return this data
    if len(stations):
        refStation = stations[refStationIndex]
    return (usedData, imagesCounter, stations, refStation, delayDict, message)

def makeServerManager(port, authkey, sbsPerNodeDict):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_p_s, get_job_p_r, get_job_q and 
        get_result_q methods.
    """
    # We create as many pipes as nodes, we use these pipes to send the job that 
    # each node must do. We also create a queue for submiting the jobs for 
    # creating images and another one to send all the results back to the server
    pipesDict = {}
    for node in sbsPerNodeDict:
        pipesDict[node] = Pipe(False) # False because we do not need duplex connection
    # Create the queue to recieve the mesages (and gains) from the clients
    resultQueue = multiprocessing.Queue()
    # Create queue to send the plot jobs to the plot-workers (this will only be used when xaxis==freq)
    jobQueue = multiprocessing.Queue()
    class JobManager(SyncManager): pass
    JobManager.register('get_job_p_r', callable=lambda k: pipesDict[k][0])
    JobManager.register('get_job_p_s', callable=lambda k: pipesDict[k][1])
    JobManager.register('get_result_q', callable=lambda: resultQueue)
    JobManager.register('get_job_q', callable=lambda: jobQueue)
    manager = JobManager(address=('', port), authkey=authkey)
    manager.start()
    print 'Server started at port %s' % port
    return manager

def main(opts):
    """ The main function. It creates the server manager and initialize the 
        remote clients (one for each node). The it runs the main server code
        and finally wait for all remote client to finish.
        It creates the INFO file and shows the command to create the movie.
    """
    # Check input GDS file
    if opts.input == "" or not os.path.isfile(opts.input): 
        print 'ERROR: You must specify a valid GDS file path.'
        print '       Use "gainanim.py -h" to get help.'
        return
    inputGDS = opts.input
    # Get the input dictioanry from the GDS file
    sbsPerNodeDict = getInputDict(inputGDS)
    numNodes = len(sbsPerNodeDict) # number of nodes which contains data
    # Check that any output folder is provided
    if opts.output == "":
        print 'ERROR: You must specify an output folder. This script will create many files!'
        print '       Use "gainanim.py -h" to get help.'
        return
    outputFolder = os.path.abspath(opts.output)
    # Read the rest of arguments
    jonesElements = getJonesElements(opts.jones)
    stationsPattern = opts.stations
    refStation = opts.refstation
    polar = not opts.cartesian
    xaxis = opts.xaxis
    figSize = getFigSize(opts.figsize)
    channelsslots = getSlots(opts.channels)
    timeslots = getSlots(opts.timeslots)
    yRange = getYRange(opts.yrange, polar)
    if opts.nodes != '':
        if xaxis == TIME:
            print 'Warning: provided extra nodes will not be used since xaxis==' + TIME
        else:
            # We add to sbsPerNodeDict the nodes which do not contain SBs but 
            # we still want them to contribute to images creation)
            for extraNode in getNodes(opts.nodes):
                if extraNode not in sbsPerNodeDict:
                    sbsPerNodeDict[extraNode] = []
    numWorkers = int(opts.numworkers)
    port = int(opts.port)
    chunkInSize = int(opts.chunkin)
    chunkOutSize = int(opts.chunkout)
    # Check the chunk sizes
    if chunkOutSize > chunkInSize:
        print 'ERROR: chunkin must be higher than chunkout.'
        print '       Use "gainanim.py -h" to get help.'
        return
    delayFile = None
    if opts.delay != '':
        delayFile = os.path.abspath(opts.delay)
    initialDelayDict = getDelayDict(delayFile) # generate initial delay dict (in further steps we will add possible missing stations with null delays)
    
    print 'Input GDS: ' + str(inputGDS)
    print 'Output folder (logs, images and info file): ' + str(outputFolder)
    print 'XAxis: ' + str(xaxis)
    print 'Jones elements: ' + str(jonesElements)
    print 'Stations filter: ' + stationsPattern
    print 'Ref. station: ' + refStation
    print 'Use polar coord.: ' + str(polar)
    print 'Times: ' + str(timeslots)
    print 'Channels: ' + str(channelsslots)
    print 'YRange: ' + str(yRange)
    print 'Delay file: ' + str(delayFile)
    print 'Num. nodes with data: ' + str(numNodes)
    if xaxis == FREQ:
        print 'Num. extra-nodes: ' + str(len(sbsPerNodeDict) - numNodes)
    print 'Num. nodes for image creation: ' + str(len(sbsPerNodeDict))
    if xaxis == FREQ:
        print 'Maximum plot-workers per node: ' + str(numWorkers)
        print 'Chunk size read-workers->server: ' + str(chunkInSize)
        print 'Chunk size server->plot-workers: ' + str(chunkOutSize)
    else:
        print 'Maximum workers per node: ' + str(numWorkers)
    print 'Figures size [inches]: ' + str(figSize)
    print
    
    # Create the outputFolder
    os.system('mkdir -p ' + outputFolder)
    
    currentNode = getHostName()
    
    # We create the manager for the server (current process) that will handle 
    # the pipes and the shared queues (in different machines)
    manager = makeServerManager(port, AUTH_KEY, sbsPerNodeDict)

    # We create one remote client in each node
    remoteClients = []
    for node in sbsPerNodeDict:
        remoteClients.append(multiprocessing.Process(target=runRemoteClient, args=(node, currentNode, port, outputFolder)))
        remoteClients[-1].start()
    if xaxis == FREQ:
        print 'All remote clients correctly initialized. Starting read-workers for loading the data...'
    else:
        print 'All remote clients correctly initialized. Starting workers for loading and plotting the data...'
    
    # Run the server code
    (usedData,plotCounter,stations,usedRefStation,delayDict,message) = runServer(manager, outputFolder, xaxis, jonesElements, stationsPattern, refStation, 
              polar, timeslots, channelsslots, yRange, numWorkers, chunkInSize, chunkOutSize, sbsPerNodeDict, initialDelayDict, figSize)
    
    # Join the processes to terminate the main process
    for i in range(len(sbsPerNodeDict)):
        remoteClients[i].join()
    print 'All remote clients finished!'    
       
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(2)
    manager.shutdown()
    
    # usedData contains the used measurement sets an dplotCounter the number of 
    # generated images
    if len(usedData) and plotCounter:
        # Create the file with info on the generated video (well, the one that 
        # should be generated from the created images)
        makeInfoFile(outputFolder + '/' + DEF_OUTPUT_INFO_NAME, usedData, xaxis, jonesElements, stations, usedRefStation, polar, timeslots, channelsslots, yRange, delayDict, message)
        # Create the movie
        showMakeMovieCommand(outputFolder, DEF_OUTPUT_ANIMATION_NAME, xaxis)
 
if __name__ == "__main__":
    print os.path.abspath(__file__),version_string
    print 
    usage = 'Usage: %prog [options]'
    description = "A tool to generate movies from the gain solutions of a calibrated observation."
    op = optparse.OptionParser(usage=usage, description=description)
    op.add_option('-i','--input',default='',help='Input GDS file. When plotting with xaxis==' + FREQ + ', it is recommended that the given GDS file does not reference more than 40 SBs. Given the resolution of the station plots, giving more SBs will not have a noticeable effect.',type='string')
    op.add_option('-o','--output',default='',help='Output folder in the current node where the logs, images and the info file will be stored. This directory must be shared between the involved nodes.',type='string')
    op.add_option('-x','--xaxis',help='X axis (choose from ' + TIME + '|' + FREQ + ') [default ' + FREQ + ']. The animation will be as a function of the other one (by default ' + TIME + ')',default=FREQ,type='choice',choices=[TIME,FREQ ])
    op.add_option('-j','--jones',default='0,3',help='Elements of the Jones matrix to plot. [default 0,3]',type='string')
    op.add_option('-s','--stations',default='*',help='Filter for stations (e.g. CS*) [default *, i.e. all]. Note that you have to escape asterisks, e.g. \"*\" or \\*.',type='string')
    op.add_option('-r','--refstation',default='',help='You can specify the reference station name to be used for the phase plot (if plotting polar coordinates). By default the first station is used.',type='string')
    op.add_option('-c','--cartesian',default=False,help='Show complex number in cartesian coordinates? [default False, i.e. show them in polar coordinates]',action='store_true')
    op.add_option('-t','--timeslots',help='Timeslots to use (comma separated and zero-based: start,end[inclusive],step) (for the last time sample specify -1). [default 0,-1,1]. If xaxis==' + TIME + ', we suggest to use large value for the step given the resolution of the station plots.',default='0,-1,1')
    op.add_option('-f','--channels',help='Channels to use in each SB (comma separated and zero-based: start,end[inclusive],step) (for the last channel specify -1). [default is to use only the first channel, i.e. 0,0,1]',default='0,0,1')
    op.add_option('-y','--yrange',help='Y range, specify four values comma separated, i.e. minAmpl,maxAmpl,minPhase,maxPhase in case of polar [default is ' + DEFAULT_POLAR_YRANGE + ' in polar and ' + DEFAULT_CARTESIAN_YRANGE + ' in cartesian]' ,default='')
    op.add_option('-d','--delay',help='Delay File [optional], file with the delays per station to be applied in the phases',default='')
    op.add_option('-n','--nodes',help='Extra nodes to use for the plot-workers (only used if xaxis==' + FREQ + '). The nodes containing the data are always used to create the images. In addition you can specify other nodes that will also contribute in the images creation. Specify the common node name and the desired node indexes. For a range use "..". For example in EoR cluster we could use node001..010,012,013. The supported common names are node,lofareor,lce,lse,dop and locus' ,default='')
    op.add_option('-w','--numworkers',default='16',help='Number of plot-workers (for pictures generation) used per node. Take into account that (if xaxis==' + FREQ + ') for each node also read-workers will be created (as many as number of SBs in the node). [default is 16]',type='string')
    op.add_option('-p','--port',default='1234',help='Port number to be used for TCP communication between current node and the nodes with the data [default is 1234]',type='string')
    op.add_option('-a','--chunkin',default='512',help='Chunk size (only used if xaxis==' + FREQ + ') of the gains (in time) requested by the server (in local machine) to the remote nodes. The higher the number the more RAM the local machine will need to combine the chunks from remote nodes. If possible, the recommended value is numworkers * numnodes * chunkout [default is 512]',type='string')
    op.add_option('-b','--chunkout',default='1',help='Chunk size (only used if xaxis==' + FREQ + ') of the combined gains (in time) send by the server (in local machine) to the remote nodes (that will generate the images). [default is 1]',type='string')
    op.add_option('-g','--figsize',default=FIG_SIZE,help='Size of the figures in inches. [default is ' + FIG_SIZE + ']',type='string')
    (opts, args) = op.parse_args()
    main(opts)