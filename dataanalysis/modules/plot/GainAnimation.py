################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, time, multiprocessing, sys
from multiprocessing import Pipe
from multiprocessing.managers import SyncManager
from ledama import utils
from ledama import diagoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.DiagnosticFile import DiagnosticFile
from ledama.MovieInfoFile import MovieInfoFile
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb.Naming import GAIN_KEY, GAIN, ID, STATION, MS, VALUES, SBINDEX,\
    CENTFREQ, LDSBP, BW, TSTEP, FSTEP
import numpy as np
import matplotlib
matplotlib.use('Agg')
import pylab

TIME = 'time'
FREQ = 'freq'
DEFAULT_CHANNELS = '0,0,1'
DEFAULT_POLAR_YRANGE = '0,0.0025,-3.14,3.14'
DEFAULT_CARTESIAN_YRANGE = '-0.0025,0.0025,-0.0025,0.0025'
AUTH_KEY = 'animationdistributionkey'
DEF_OUTPUT_INFO_NAME = 'INFO'
COORD_NUM = 2
FIG_SIZE = '12,7'

# MESSAGE FROM WORKERS (CHILDREN IN CLIENTS)
MESS_TYPE_WORKER_OK_GAIN = 0
MESS_TYPE_WORKER_KO_GAIN = 1
MESS_TYPE_WORKER_SEND_GAIN = 2
MESS_TYPE_WORKER_IMAGE_CREATED = 3
MESS_TYPE_WORKER_END = 4
# MESSAGE FROM CLIENTS
MESS_TYPE_CLIENT_END = 5

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

def getGain(clientMSId, gainIds, gainPartTable, sbCentFreq, sbBW, freqStep, timeStep, stations, jonesElements, polar, tIndexInit, tIndexEnd, tIndexStep, cIndexInit, cIndexEnd, cIndexStep, timeout, dbname, dbuser, dbhost):
    """Get the gain solutions indicated by gainIds from the LEDDB"""
    try:
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        qm = QueryManager()
        queryOption = GAIN_KEY
        names = [STATION, VALUES]
        qc = qm.initConditions()
        qm.addCondition(queryOption, qc, MS+ID, clientMSId, '=')          
        qm.addCondition(queryOption, qc, GAIN+ID, tuple(gainIds))
        
        (query, queryDict) = qm.getQuery(queryOption, qc, names, [STATION,])
        
        #query = query.replace(GAIN+' ', gainPartTable + ' ').replace(GAIN+'.', '') # trick to directly query to the partition that has the data
        cursor = connection.cursor()
        print query
        print qm.executeQuery(connection, cursor, query, queryDict, True, timeout)
        qm.executeQuery(connection, cursor, query, queryDict, False, timeout)
        (Gains, times, freqs) = (None,None,None)
        for row in cursor:
            rowDict = qm.rowToDict(row, names)
            stationIndex = stations.index(rowDict.get(STATION))
            # values is corr,freq,time,cord
            rowValues = rowDict.get(VALUES)
            jonesValues = []
            for jones in jonesElements:
                jonesValues.append(rowValues[jones])
            # jonesValues is corr,channel,times,cord(real,imag) (only the select jones)
            rowGains = utils.convertValues(jonesValues, np.complex64)
            # rowGains is corr,channel,times (with numpy.complexs)
            if Gains == None:
                # Theoretically Gains will be None in the first row. So, then we 
                # read this information since, in principle, it is the same 
                # for all the rows. We also create the large Gain matrix
                numAllCorrs = len(rowGains)
                numAllFreqs = len(rowGains[0])
                numAllTimes = len(rowGains[0,0])
                # Compute the initial frequency of the channels
                allfreqs = (np.array(range(0,numAllFreqs)) * float(freqStep)) + (sbCentFreq - (sbBW / 2.))
                if cIndexEnd == -1 or cIndexEnd > numAllFreqs:
                    cIndexEnd = numAllFreqs
                else:
                    cIndexEnd += 1
                freqs = allfreqs[cIndexInit:cIndexEnd:cIndexStep] # Get only the requested channels
                Nf = len(freqs)
                # Compute the time samples relative to initial time of observing
                alltimes = np.array(range(0,numAllTimes)) * float(timeStep)
                if tIndexEnd == -1 or tIndexEnd > numAllTimes:
                    tIndexEnd = numAllTimes
                else:
                    tIndexEnd += 1
                
                times = alltimes[tIndexInit:tIndexEnd:tIndexStep]
                Nt = len(times)
                # Create a full matrix with zeros for all Gains solutions to be read
                Gains = np.zeros((len(stations),numAllCorrs,Nf,Nt),dtype=np.complex64)
            # We fill in the matrix the part related to the current station
            Gains[stationIndex,:,:,:] = rowGains[:,cIndexInit:cIndexEnd:cIndexStep,tIndexInit:tIndexEnd:tIndexStep]
        cursor.close()
        connection.close()
        if polar:
            Gains = np.array((np.abs(Gains),np.angle(Gains)))
        else:
            Gains = np.array((np.real(Gains),np.imag(Gains)))
        # Gains is cord, station, jones, freq, times    
        Gains = Gains.transpose((3,4,1,0,2))
        # We return freq, times, station, cord, jones
        return (Gains, times, freqs)
    except Exception,e:
        print 'ERROR getting gain: ' + str(e)
        sys.stdout.flush()
    return (None,None,None)

def runPlotWorker(workerName, sharedJobQueue, sharedResultQueue, stations, outputFolder, times, polar, refStationIndex, acoord, margx, margy, ylim, dx, dy, delay, figSize):
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
                    imageTitle = "t=%.1f, Freqs=[%5.1f, %5.1f]MHz" % (times[(ti+tir)],minFreq,maxFreq)
                    if polar:
                        imageTitle += ", RefStation=%s" % refStation
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

def runReadWorker(workerName, recvJobPipeRW, sharedResultQueue, clientMSId, msIdData, stations, jonesElements, polar, tIndexInit, tIndexEnd,tIndexStep, cIndexInit,cIndexEnd,cIndexStep, chunkSize, timeout, dbname, dbuser, dbhost, dbinterval):
    """ Read Worker that is executed in remote clients when xaxis=freq. 
        First it loads its realted SB and then, under request, it sends the 
        chunks of data to the main server
        """
    (gainIds, (gainPartTable, sbCounter, sbIndex, sbCentFreq, sbBW, timeStep , freqStep)) = msIdData
    try:
        time.sleep(sbCounter*dbinterval)
        # Loads the gains (dimensions are freq, time, station, cord, jones)
        (gains, times, freqs) = getGain(clientMSId, gainIds, gainPartTable, sbCentFreq, sbBW, freqStep, timeStep, stations, jonesElements, polar, tIndexInit, tIndexEnd,tIndexStep, cIndexInit,cIndexEnd,cIndexStep,timeout, dbname, dbuser, dbhost)
        # Check if data was successfully loaded
        if gains == None:
            message = workerName + ' could not load gains from LEDDB for MS ID ' + str(clientMSId)
            sharedResultQueue.put([MESS_TYPE_WORKER_KO_GAIN, message,],)
        else:
            # Send message that we are done reading the gains, so we are ready to starting 
            # the plotters and the queries for time chunks
            message = workerName + ' loaded gains from LEDDB for MS ID ' + str(clientMSId) + (' (%5.1f MHz' % float(sbCentFreq / 1e6)) + ')'
            sharedResultQueue.put([MESS_TYPE_WORKER_OK_GAIN, message, clientMSId, sbIndex, freqs, times])
        log(message)
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
    
def runWorker(workerName, node, clientJobQueue, sharedResultQueue, stations, jonesElements, polar, tIndexInit, tIndexEnd, tIndexStep, cIndexInit, cIndexEnd, cIndexStep, refStationIndex, yRange, delay, outputFolder, figSize, timeout, dbname, dbuser, dbhost, dbinterval):
    """ Worker that is executed in the remote clients when xaxis==time. 
        They get from the queue the SB, and for each one they create their 
        related images"""
    kill_received = False
    numStations = len(stations)
    refStation = stations[refStationIndex]
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
                [clientMSId, msIdData] = job
                (gainIds, (gainPartTable, sbCounter, sbIndex, sbCentFreq, sbBW, timeStep , freqStep)) = msIdData
                time.sleep(sbCounter*dbinterval)
                # Loads the gains (freq, time, station, cord, jones)
                (gains, times, freqs) = getGain(clientMSId, gainIds, gainPartTable, sbCentFreq, sbBW, freqStep, timeStep, stations, jonesElements, polar, tIndexInit, tIndexEnd,tIndexStep, cIndexInit,cIndexEnd,cIndexStep,timeout, dbname, dbuser, dbhost)
                if gains == None:
                    message = workerName + ' could not load gains from LEDDB for MS ID ' + str(clientMSId)
                    log(message)
                    sharedResultQueue.put([MESS_TYPE_WORKER_KO_GAIN, message,],)
                else:
                    if minTime == None: 
                        (minTime,maxTime) = (times[0],times[-1]) 
                    # Initialize the figure and get layout parameters
                    log(workerName + ' loaded gains from LEDDB for MS ID ' + str(clientMSId) + (' (%5.1f MHz' % float(sbCentFreq / 1e6)) + '). Starting plotting...')
                    (acoord, margx, margy, ylim, dx, dy) = getPlotArgs(numStations, yRange)
                    addposy = [margy, dy]
                    fig = pylab.figure(figsize=figSize)
                    # For all the channel indexes we create images
                    for ci in range(len(gains)):
                        # Clean the figure
                        pylab.clf()
                        fig.clf()
                        imageName = outputFolder + ('/timg%03d%03d.png' % (sbIndex,ci))
                        Jones2Ref = None
                        if polar:
                            Jones2Ref = gains[ci,:,refStationIndex,1] 
                        for antIndex in range(numStations):
                            stationName = stations[antIndex]
                            stationGains = gains[ci,:,antIndex] #stationGains is time, cord, jones
                            if polar:
                                stationGains[:,1] -= Jones2Ref
                                stationGains[:,1] -= 2 * np.pi * freqs[ci] * 1e6 * delay[stationName]
                                stationGains[:,1] = np.remainder(stationGains[:,1],2*np.pi) - np.pi
                            a = None
                            # for (Real and Imag) or (Ampl and Phase)
                            for cordIndex in range(COORD_NUM):
                                # we create a sub-plot
                                a = fig.add_axes([acoord[antIndex,0]+margx, acoord[antIndex,1]+addposy[cordIndex], dx-2.*margx,dy-margy])
                                # for each element in the jones matrix (that we selected to plot)
                                for j in range(len(stationGains[0][cordIndex])):
                                    a.plot(times,stationGains[:,cordIndex,j],marker='.',ls='',mew=0.1,ms=1.5,color='black')
                                pylab.setp(a, xticks=[], yticks=[],ylim=ylim[cordIndex])
                                setLabels(a, polar, antIndex, cordIndex, "Time")
                            a.title.set_text(stationName)
                            a.title.set_fontsize(9)
                        # Set general title
                        imageTitle = "f=%.1fMHz, Times=[%5.1f, %5.1f]s" % (freqs[ci]/1e6,minTime,maxTime)
                        if polar:
                            imageTitle += ", RefStation=%s"%refStation
                        a.annotate(imageTitle, xy=(0.5, 0.97),  xycoords='figure fraction', horizontalalignment="center")
                        # Save the image
                        fig.savefig(imageName)
                        log(workerName + ' ' + imageName + ' created')
                        # Put in the output queue that we created a new image
                        sharedResultQueue.put([MESS_TYPE_WORKER_IMAGE_CREATED,]) 
                    # Send message to indicate we are done with this SB
                    sharedResultQueue.put([MESS_TYPE_WORKER_OK_GAIN, clientMSId, freqs[ci], sbIndex, freqs])
        except:
            # if there is an error we will quit the generation
            kill_received = True
    sharedResultQueue.put([MESS_TYPE_WORKER_END,])
    log(workerName + ' exiting...')

def runClient(snode, port):
    """ The function which is run in each remote client. 
        First of all, from the recv_job_p pipe the client receives the info on 
        which (and how) gains solutions (measurement set) to query from LEDDB.
        Then, depending on xaxis:
         1 - if xaxis == time, we create processes in charge of both querying 
         and plotting. When they are done, they will the server know and the 
         server will send message to main client process indicating that it can
         terminate its execution.
         2 - if xaxis == freqs, we create other processes (read-workers) 
         in charge of querying the data. There are as many read-workers as SBs 
         assigned to this node. Then these new read-workers load in local memory 
         the gains solutions and let the server know that they are ready to start 
         further processing. Then, the main client process will create other 
         processes in charge of creation of images (plot-workers). Third, it 
         remains listening to the recv_job_p where the server will query for 
         chunks of the loaded gains and forward these info to read-workers. 
         When all the gains solutions have been sent (under server request) and 
         all the plot-workers have finished their plotting tasks the life of 
         this main process is over
    """
    manager = makeClientManager(snode, int(port), AUTH_KEY)
    node = utils.getHostName()
    print 'Getting job receiving pipe'
    recvJobPipe = manager.get_job_p_r(node)
    print 'Getting queues'
    sharedResultQueue = manager.get_result_q()
    # First data we receive is the info
    print 'Getting initial data...'
    data = recvJobPipe.recv()
    outputFolder = data['OUTPUT']
    mssDict= data['MSDICT']
    numWorkers = data['NUMWORKERS']
    xaxis = data['XAXIS']
    jonesElements = data['JONES']
    stations = data['STATIONS']
    refStationIndex = data['REFSTATIONINDEX']
    polar = data['POLAR']
    timeslots = data['TIMES']
    channelsslots = data['CHANNELS']
    yRange = data['YRANGE']
    chunkSize = data['TIMECHUNKSIZE']
    delay = data['DELAYS']
    figSize = data['FIGSIZE']
    timeout = data['TIMEOUT']
    dbname = data['DBNAME']
    dbuser = data['DBUSER']
    dbhost = data['DBHOST']
    dbinterval = data['DBINTERVAL']
    
    tIndexInit,tIndexEnd,tIndexStep = timeslots
    cIndexInit,cIndexEnd,cIndexStep = channelsslots
    
    workers = []
    workersNames = []
    if xaxis == TIME:
        # Create queue in client node to comunicate with workers
        clientJobQueue = multiprocessing.Queue()
        for clientMSId in mssDict:
            clientJobQueue.put([clientMSId,mssDict[clientMSId]])
        for i in range(numWorkers):
            workersNames.append(node + ('-worker%02d' % i))
            clientJobQueue.put(None) # we put a None in the job for each worker (they are used to indicate them to finish)
            workers.append(multiprocessing.Process(target=runWorker, 
                args=(workersNames[-1], node, clientJobQueue, sharedResultQueue, stations, jonesElements, polar, 
                      tIndexInit, tIndexEnd, tIndexStep, cIndexInit, cIndexEnd, cIndexStep, 
                      refStationIndex, yRange, delay, outputFolder, figSize, timeout, dbname, dbuser, dbhost, dbinterval)))
            workers[-1].start()
    else: # xaxis == FREQ
        sharedJobQueue = manager.get_job_q()
        numReadWorkers = len(mssDict)
        clientMSIds = mssDict.keys()
        numPlotWorkers = numWorkers
        log('Starting ' + str(numReadWorkers) + ' read-workers')
        sendJobPipeRWs = []
        for i in range(numReadWorkers):
            workersNames.append(node + ('-readWorker%02d' % i))
            # Create pipe to communicate the main client process with the several read-workers
            recvJobPipeRW, sendJobPipeRW = Pipe(False)
            sendJobPipeRWs.append(sendJobPipeRW)
            workers.append(multiprocessing.Process(target=runReadWorker, 
                args=(workersNames[-1], recvJobPipeRW, sharedResultQueue, clientMSIds[i], mssDict[clientMSIds[i]], 
                      stations, jonesElements, polar, tIndexInit, tIndexEnd, tIndexStep, 
                      cIndexInit,cIndexEnd,cIndexStep, chunkSize, timeout, dbname, dbuser, dbhost, dbinterval)))
            workers[-1].start()
        # Once server has acknowledge of initialization of all SBs in all nodes
        # we will get a message with indications 
        data = recvJobPipe.recv()
        times = data['TIMES']
        log('Starting ' + str(numPlotWorkers) + ' plot workers')
        # Compute the variable needed for plots
        (acoord, margx, margy, ylim, dx, dy) = getPlotArgs(len(stations), yRange)
        for i in range(numPlotWorkers):
            workersNames.append(node + ('-plotWorker%02d' % i))
            workers.append(multiprocessing.Process(target=runPlotWorker,
                    args=(workersNames[-1], sharedJobQueue, sharedResultQueue, stations, outputFolder, 
                          times, polar, refStationIndex, acoord, margx, margy, ylim, dx, dy, delay, figSize)))
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

def makeClientManager(server, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address:port and exposes the get_job_p_r, get_job_q and 
        get_result_q methods for accessing the pipes and shared queue from the 
        server. Return a manager object.
    """
    class JobManager(SyncManager): pass
    JobManager.register('get_job_p_r')
    #JobManager.register('get_job_p_s') #NOT REQUIRED
    JobManager.register('get_job_q')
    JobManager.register('get_result_q')
    manager = JobManager(address=(server, port), authkey=authkey)
    manager.connect()
    print 'Client connected to %s:%s' % (server, port)
    sys.stdout.flush()
    return manager

def log(message):
    """ Method that print to stdout and automatically flushes it"""
    sys.stdout.write(message + '\n')
    sys.stdout.flush()
    
class GainAnimation(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('diagfile', 'i', 'Input DiagFile', helpmessage='. When plotting with xaxis==' + FREQ + ', it is recommended that the given DiagFile does not reference gain solutions from more than 40 SBs. Given the resolution of the station plots, giving more SBs will not have a noticeable effect.')
        options.add('output', 'o', 'Output folder', helpmessage=' in the current node where the logs, images and the info file will be stored. This directory must be shared between the used nodes.')
        options.add('xaxis', 'x', 'X axis', choice=[FREQ,TIME], default=FREQ, helpmessage='. The animation will be as a function of the other one (by default ' + TIME + ')')
        options.add('jones', 'j', 'Jones', default='0,3', helpmessage='. Elements of the Jones matrix to plot.')
        options.add('refstation', 'r', 'Reference Station', mandatory=False, helpmessage='. You can specify the reference station name to be used for the phase (if plotting polar coordinates). If not specified, the first station is used.')
        options.add('cartesian', 'c', 'Plot cartesian?', default=False,helpmessage=' instead of polar coordinates')
        options.add('timeslots', 't', 'Timeslots to use',default='0,-1,1',  helpmessage=' (comma separated and zero-based: start,end[inclusive],step) (for the last time sample specify -1). If xaxis==' + TIME + ', we suggest to use large value for the step given the resolution of the station plots.')
        options.add('channels', 'f', 'Channels to use', mandatory = False, helpmessage=' in each SB (comma separated and zero-based: start,end[inclusive],step) (for the last channel specify -1). [default is to use only the first channel, i.e. 0,0,1]')
        options.add('yrange', 'y', 'Y range', mandatory=False, helpmessage=', specify four values comma separated, i.e. minAmpl,maxAmpl,minPhase,maxPhase in case of polar [default is ' + DEFAULT_POLAR_YRANGE + ' in polar and ' + DEFAULT_CARTESIAN_YRANGE + ' in cartesian]')
        options.add('nodes', 'n', 'Nodes to use', helpmessage=' to use for the remote clients (and its workers).')
        options.add('numworkers', 'w', 'Num. plot-workers', default = 16, helpmessage=' (for pictures generation) used per node. Take into account that, if xaxis==' + FREQ + ', for each node also read-workers (specific worker querying the LEDDB) will be created (as many as number of SBs in the node). In the case of xaxis==' + TIME + ', the plot-workers are also in charge of LEDDB queries')
        options.add('port', 'p', 'Port', default = 1234, helpmessage=' number to be used for TCP communication between current node and the nodes with the data')
        options.add('chunkin', 'a', 'Chunk Size In', default = 512, helpmessage=' (only used if xaxis==' + FREQ + ') of the gains (in time) requested by the server (in local machine) to the remote nodes. The higher the number the more RAM the local machine will need to combine the chunks from remote nodes. If possible, the recommended value is numworkers * numnodes * chunkout [default is 512]')
        options.add('chunkout', 'b', 'Chunk Size Out', default = 1, helpmessage=' (only used if xaxis==' + FREQ + ') of the combined gains (in time) send by the server (in local machine) to the remote nodes (that will generate the images).')
        options.add('delay', 'd', 'Delay File', mandatory=False, helpmessage=', file with the delays per station to be applied in the phases')
        options.add('figsize', 'g', 'Size of the figures', default=FIG_SIZE, helpmessage=' in inches.')
        options.add('dbtimeout', 'm', 'DB connection timeout', default = 300)
        options.add('dbname', 'l', 'DB name', default=DEF_DBNAME)
        options.add('dbuser', 'u', 'DB user', default=self.userName)
        options.add('dbhost', 'z', 'DB host', default=DEF_DBHOST)
        options.add('dbinterval', 'e', 'DB interval', default=1.5,helpmessage=' (in seconds) between LEDDB queries.')                
        # the information
        information = """Generate a movie of all the stations gains."""
        # Initialize the parent class
        LModule.__init__(self, options, information)   

        self.anim = None
        
    def getSlots(self, slots):
        """ Parse the slots from the args to a list of ints.
            It returns: [initialIndex, endIndex, step] """
        slotssplit = slots.split(',')
        if len(slotssplit) != 3:
            raise Exception('ERROR: slots format is start,end,step')
        for i in range(len(slotssplit)): slotssplit[i] = int(slotssplit[i])
        return slotssplit
    
    def getYRange(self, yrange, polar):
        """ Parse the yrange from the args to a list of floats 
            (if required, it assigns the default values)
        """
        if yrange == '':
            if polar:
                yrange = DEFAULT_POLAR_YRANGE
            else:
                yrange = DEFAULT_CARTESIAN_YRANGE
        yrangesplit = yrange.split(',')
        if len(yrangesplit) != 4:
            raise Exception('Error: YRange format is min1,max1,min2,max2')
        for i in range(len(yrangesplit)):
            yrangesplit[i] = float(yrangesplit[i])
        return yrangesplit
    
    def getFigSize(self, figsize):
        figsize = figsize.split(',')
        if len(figsize) != 2:
            raise Exception('ERROR: figsize format is xsize,ysize')
        for i in range(len(figsize)): figsize[i] = int(figsize[i])
        return tuple(figsize)
    
    def getDelayDict(self, delayFile, stations):
        """Get a delay dictionary from the given file.
        It returns all 0 if none file is provided"""
        delayDict = {}
        for station in stations:
            delayDict[station] = 0.
        if delayFile != None:
            lines = open(delayFile,'r').read().split('\n')
            for line in lines:
                if line != '':
                    (station,delay) = line.split()[:2]
                    if station in stations:
                        delayDict[station] = float(delay)
        return delayDict
    
    def logImagesProgress(self, counter, total, numWorkers):
        """ Show dynamic progress of created images"""
        message = "\rImages completed: %d of %d (%3.1f%%). Num. workers: %4d" % (counter,total, float(counter) * 100./float(total), numWorkers)
        sys.stdout.write(message)
        sys.stdout.flush()
        
    def showMakeMovieCommand(self, outputFolder, animFile, xaxis):
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
    
    def runRemoteClient(self, node, snode, port, outputFolder):
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

    def runServer(self, manager, outputFolder, xaxis, jonesElements, stations, refStationIndex, polar, timeslots, channelsslots, yRange, numWorkers, chunkInSize, chunkOutSize, mssPerNodeDict, delayDict, figSize, timeout, dbname, dbuser, dbhost, dbinterval):
        """ The server which is run in current machine. From the manager we get the 
            involved pipes and queues. We send to the remote clients a message to 
            processing the gain solutions
            Then, depending on xaxis:
            1- For xaxis==time each client will create workers (as many as numWorkers).
              Each worker will process a SB (or more) and for each SB create its 
              related images. We do not require further communication in this case
            2- For xaxis==freq we need communication with central server to join, 
             for each time sample, all the solutions from different SBs.
             So, each remote client will create as many read-workers as SBs assigned to the node.
             Then we split the times in chunks and request the gains solutions to all 
             the read-wrokers for each chunk. When we have all the gains (for a given chunk) 
             for all SBs, we summit to the job queue all the tasks for the plot workers
        """
        numClients = len(mssPerNodeDict)
        finishClientCounter = 0
        
        imagesCounter = 0
        usedData = []
        message = ''
        
        finishWorkersCounter = 0
        numTotalWorkers = None
        
        numSBs = 0
        sendJobsPipes = {}
        for node in mssPerNodeDict:
            sendJobsPipes[node] = manager.get_job_p_s(node)
            numSBs += len(mssPerNodeDict[node])
            # We send the messages on what data to load to all the clients
            data = {}
            data['OUTPUT'] = outputFolder
            data['MSDICT'] = mssPerNodeDict[node]
            data['NUMWORKERS'] = numWorkers
            data['XAXIS'] = xaxis
            data['JONES'] = jonesElements
            data['STATIONS'] = stations
            data['REFSTATIONINDEX'] = refStationIndex
            data['POLAR'] = polar
            data['TIMES'] = timeslots
            data['CHANNELS'] = channelsslots
            data['YRANGE'] = yRange
            data['TIMECHUNKSIZE'] = chunkInSize
            data['FIGSIZE'] = figSize
            data['DELAYS'] = delayDict
            data['TIMEOUT'] = timeout
            data['DBNAME'] = dbname
            data['DBUSER']  = dbuser
            data['DBHOST'] = dbhost
            data['DBINTERVAL'] = dbinterval
            sendJobsPipes[node].send(data) # We send the messages with details to all the clients
            
        # The queue to receive gains and message from remote clients (from their read-workers more concretely)
        sharedResultQueue = manager.get_result_q()    
        
        if xaxis == TIME:
            # We can close the pipes since in this case we do not need them anymore
            for node in mssPerNodeDict:
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
                    [clientMSId, freq, sbIndex, freqs] = rmess[1:]
                    if clientMSId not in numChannelsDict:
                        # First time we recieve data from certain SB
                        numChannelsDict[clientMSId] = len(freqs)
                        usedData.append((clientMSId, freq, sbIndex))
                elif rmessType == MESS_TYPE_WORKER_IMAGE_CREATED:
                    imagesCounter += 1 
                elif rmessType == MESS_TYPE_CLIENT_END: # Clients where all the workers finished
                    finishClientCounter += 1
                self.logImagesProgress(imagesCounter, numSBs * len(freqs), numTotalWorkers - finishWorkersCounter)
            print # we print blank line after the dynamic writting
            # Show possible accumulated error messages
            if len(errorMessages):
                print str(len(errorMessages)) + ' SBs had errors while querying Gains:'
                for errorMessage in errorMessages:
                    print ' ' + errorMessage
            
            # All workers finished loading and plotting data. Waiting clients finalization
            while (finishClientCounter < numClients):
                try:
                    sharedResultQueue.get() # They are all MESS_TYPE_CLIENT_END_WORKERS messages
                    finishClientCounter += 1
                except:
                    pass
            
            if len(numChannelsDict): # check that all SBs had the same number of channels
                numChannels = numChannelsDict[numChannelsDict.keys()[0]]
                for clientMSId in numChannelsDict:
                    if numChannelsDict[clientMSId] != numChannels:
                        message = 'WARNING: GAINS MAY NOT BE COHERENT (different SBs having different number of channels)'
                        print message
                        break
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
                    
                    [omessage, clientMSId, sbIndex, freqs, times] = rmess[1:]
                    usedData.append((clientMSId, freqs[0], sbIndex))
                    print omessage
                elif rmessType == MESS_TYPE_WORKER_END:
                    finishWorkersCounter += 1
                readWorkersCounter += 1
            numCorrectSBs = len(usedData) # There will be as many active read-workers as SBs with Gains
            numTimes = len(times)
            log('All clients (read-workers) finished loading data. Starting querying chunks and creating images. Num. freqs (read-workers) = ' + str(numCorrectSBs) + ', Num. stations = ' + str(len(stations)) + ', Num. times = ' + str(numTimes) + '...')

            # Send to the clients the details of the readed data
            data = {}
            data['TIMES'] = times
            for node in mssPerNodeDict:
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
                for node in mssPerNodeDict:
                    sendJobsPipes[node].send(sdata)
                # we wait until we receive all the gains from all the clients 
                while receivedGain < numCorrectSBs:
                    rmess = sharedResultQueue.get()
                    rmessType = rmess[0]
                    if rmessType in (MESS_TYPE_WORKER_IMAGE_CREATED,MESS_TYPE_CLIENT_END):
                        if rmessType == MESS_TYPE_WORKER_IMAGE_CREATED:
                            # in the meanwhile it may happend that we receive messages of finished images
                            imagesCounter += 1
                        else: # rmessType == MESS_TYPE_CLIENT_END_WORKERS:
                            # in the meanwhile it may happen that we receive messaged of
                            # clients that have finished their tasks (readers and plotters)
                            finishClientCounter += 1
                        self.logImagesProgress(imagesCounter, numTimes, numWorkers * (numClients - finishClientCounter))
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
            for node in mssPerNodeDict:
                sendJobsPipes[node].send(sdata)
            # in this point we are done reading all the gains from remote clients
            # We can close the pipes since we already have all of them ready
            for node in mssPerNodeDict:
                sendJobsPipes[node].close()
            # We add in the end of the queue as many None as numworkers, this is the 
            # ending task of each plot-worker and it is used to tell them theirs tasks are done!
            for node in mssPerNodeDict:
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
                    self.logImagesProgress(imagesCounter, numImages, numWorkers * (numClients - finishClientCounter))
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
        return (usedData, imagesCounter, message)

    def makeServerManager(self, port, authkey, nodeDict):
        """ Create a manager for the server, listening on the given port.
            Return a manager object with get_job_p_s, get_job_p_r, get_job_q and 
            get_result_q methods.
        """
        # We create as many pipes as nodes, we use these pipes to send the job that 
        # each node must do. We also create a queue for summiting the jobs for 
        # creating images and another one to send all the results back to the server
        pipesDict = {}
        for node in nodeDict:
            # False because we do not need duplex connection
            pipesDict[node] = Pipe(False)
        # Create the queue to recieve the mesages (and gains) from the clients
        resultQueue = multiprocessing.Queue()
        # Create queue to send the plot jobs to the plot-workers
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


    def process(self,diagfile,output,xaxis,jones,refstation,cartesian,timeslots,channels,yrange,nodes,numworkers,port,chunkin,chunkout,delay,figsize,dbtimeout,dbname,dbuser,dbhost,dbinterval):
        diagfile = os.path.abspath(diagfile)
        if not os.path.isfile(diagfile):
            print 'Input diagnostics file not found!'
            return
        outputFolder = os.path.abspath(output)
        # Read the rest of arguments
        jonesElements = utils.getElements(jones)
        polar = not cartesian
        timeslots = self.getSlots(timeslots)
        if channels == '':
            channels = DEFAULT_CHANNELS
        channelsslots = self.getSlots(channels)
        yRange = self.getYRange(yrange, polar)
        nodes = utils.getNodes(nodes)
        numNodes = len(nodes)
        # Check the chunk sizes
        if chunkout > chunkin:
            print 'ERROR: chunkin must be higher than chunkout.'
            return
        delayFile = None
        if delay != '':
            delayFile = os.path.abspath(delay)
        figSize = self.getFigSize(figsize)
        print 
        print 'Input DiagFile: ' + str(diagfile)
        print 'Output folder (logs, images and info file): ' + str(outputFolder)
        print 'XAxis: ' + str(xaxis)
        print 'Jones elements: ' + str(jonesElements)
        print 'Use polar coord.: ' + str(polar)
        print 'Times: ' + str(timeslots)
        print 'Channels: ' + str(channelsslots)
        print 'YRange: ' + str(yRange)
        print 'Delay file: ' + str(delayFile)
        print 'Num. nodes: ' + str(numNodes)
        if xaxis == FREQ:
            print 'Maximum plot-workers per node: ' + str(numworkers)
            print 'Chunk size read-workers->server: ' + str(chunkin)
            print 'Chunk size server->plot-workers: ' + str(chunkout)
        else:
            print 'Maximum workers per node: ' + str(numworkers)
        print 'Figures size [inches]: ' + str(figSize)
        print
        
        # Create the outputFolder
        os.system('mkdir -p ' + outputFolder)
        
        currentNode = utils.getHostName()

        # We do an initial query just to find the involved gainIds 
        # and check if they are related to same LDSBP (and have same timeStep)
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        
        # We get the different partitions of the GAIN table
        (partitionsSuffix, nodePoints) = diagoperations.getPartitionSchema(connection)
            
        qm = QueryManager()
        diagFile = DiagnosticFile(diagfile)
        names = [GAIN+ID,LDSBP+ID,MS+ID,SBINDEX,CENTFREQ,BW, STATION,TSTEP,FSTEP]
        (query, queryDict) = qm.getQuery(diagFile.queryOption, diagFile.queryConditions, names)
        cursor = connection.cursor()
        qm.executeQuery(connection, cursor, query, queryDict, timeout=dbtimeout)
    
        # We create a dictionary for the gainIds, we separate them by station
        mssDict = {}
        stationsSet = set([])
        ldsbpId = None
        timeStep = None
        freqStep = None
        for row in cursor:
            msId = row[2]
            stationsSet.add(row[6])
            if msId not in mssDict:
                # we extract the freq-time info, then we do not need to do it again
                mssDict[msId] = ([],(diagoperations.getPartition(GAIN, partitionsSuffix, nodePoints, msId), len(mssDict), row[3], row[4] * 1e6, row[5] * 1e6, row[7], row[8])) # sbIndex, centFreq, BW, timeStep , freqStep
            mssDict[msId][0].append(row[0])
            if ldsbpId == None:
                ldsbpId = row[1]
                timeStep = row[7]
                freqStep = row[8]
            elif (ldsbpId != row[1]) or (timeStep != row[7]) or (freqStep != row[8]):
                print 'Gain solutions must be related to the same LDSBP! (and have the same timeStep and freqStep)'
                return
        cursor.close()
        connection.close()
        timeStep = float(timeStep)
        freqStep = float(freqStep)
        stations = sorted(stationsSet)
        refStationIndex = 0
        if refstation != '' and refstation in stations:
            refStationIndex = stations.index(refstation)
        # Get the delay dictionary
        delayDict = self.getDelayDict(delayFile, stations)
        
        print 'Num. MSs: ' + str(len(mssDict))
        print 'Num. Stations: ' + str(len(stations))
        print 'Ref. station: ' + stations[refStationIndex]
        print 'Times step: ' + str(timeStep)
        print 'Freq. step: ' + str(freqStep)
        print
        # We create a many sub-dict, one for each node
        # hence, each node will have assigned a list of MSs
        # and will be in charge of the gains related to them
        nodesDictList = utils.splitDictionary(mssDict, numNodes)
        mssPerNodeDict = {}
        for i in range(numNodes):
            mssPerNodeDict[nodes[i]] = nodesDictList[i]
                   
        # We create the manager for the server (current process) that will handle 
        # the pipes and the shared queues 
        manager = self.makeServerManager(port, AUTH_KEY, mssPerNodeDict)
    
        # We create one remote client in each node
        remoteClients = []
        for node in nodes:
            remoteClients.append(multiprocessing.Process(target=self.runRemoteClient, args=(node, currentNode, port, outputFolder)))
            remoteClients[-1].start()
        if xaxis == FREQ:
            print 'All remote clients correctly initialized. Starting read-workers for querying LEDDB data...'
        else:
            print 'All remote clients correctly initialized. Starting workers for querying LEDDB and plotting the data...'    
        
        # Run the server code manager
        (usedData,plotCounter,message) = self.runServer(manager, outputFolder, xaxis, jonesElements, stations, refStationIndex, 
                  polar, timeslots, channelsslots, yRange, numworkers, chunkin, chunkout, mssPerNodeDict, delayDict, figSize, dbtimeout, dbname, dbuser, dbhost, dbinterval)
        
        # Join the processes to terminate the main process
        for i in range(numNodes):
            remoteClients[i].join()
        print # we print a blank line after the dynamic writting in stdout
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
            MovieInfoFile(outputFolder + '/' + DEF_OUTPUT_INFO_NAME).write(usedData, xaxis, jonesElements, stations, stations[refStationIndex],  polar, timeslots, channelsslots, yRange, delayDict, message)
            # Create the movie
            self.showMakeMovieCommand(outputFolder, os.path.basename(diagfile).split('.')[0] + '.mp4', xaxis)
