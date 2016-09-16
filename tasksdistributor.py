#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, multiprocessing, sys, subprocess

# Code to distribute tasks using multiprocessing
# tasks are defined by ids and whats. Both are list and must have same length
# tasks are grouped by ids. Hence, different "whats" can have same id
# Some children are created. Each child will be in charge of some ids
# Each child will also create some grandchildren for processing its related whats
# The user can define the number of children in action and the number of 
# grandchildren created for each child

FINISHED_OK = 0
FINISHED_KO = 1
DEV_NULL = '/dev/null'

def showOk(retValuesOk, showAllIfTuple = False, message = None):
    if message != None and len(retValuesOk):
        print message
    for retValueOk in sorted(retValuesOk):
        if retValueOk != None:
            if type(retValueOk) == tuple:
                if showAllIfTuple:
                    print '   ' + ' '.join(retValueOk)
                else:
                    print retValueOk[-1]
            else:
                print retValueOk
def showKoAll(retValuesKo):
    if len(retValuesKo):
        print 'Errors:'
        for retValueKo in retValuesKo:
            print ' ' + str(retValueKo)  
def showKoFirst(retValuesKo):
    if len(retValuesKo):
        print 'There were some errors. Example:'
        print retValuesKo[0] 

def logProgress(finished, total, ko, numPerId, numFinishedPerId):
    finishedIds = 0
    for identifier in numPerId:
        if numPerId[identifier] == numFinishedPerId[identifier]:
            finishedIds+=1
    if ko:
        sys.stdout.write("\r Tasks finished: %d of %d (%3.1f%%), %d of %d children. With errors: %d " % (finished, total, float(finished) * 100./float(total), finishedIds, len(numPerId), ko))
    else:
        sys.stdout.write("\r Tasks finished: %d of %d (%3.1f%%), %d of %d children. " % (finished, total, float(finished) * 100./float(total), finishedIds, len(numPerId)))
    sys.stdout.flush()     

def execute(command, redirect=False):
    sys.stdout.flush()
    sys.stderr.flush()
    if redirect and sys.stdout.name != '<stdout>':
        if command.count('>'):
            raise Exception('command already contains redirection of stdout')
        return subprocess.Popen(command, shell = True, stdout=sys.stdout, stderr=sys.stdout).communicate()
    else:
        return subprocess.Popen(command, shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

def distribute(ids, whats, functionToApply, maxGrandChildren, maxChildren, logFolder = DEV_NULL, logBaseFileNames = None, getLogFileName = None, dynamicLog = True):
    numTasks = len(ids)
    if numTasks != len(whats):
        raise Exception('len(ids) != len(whats)')
    
    if logFolder not in (None, '', DEV_NULL):
        os.system('mkdir -p ' + logFolder)
        print 'Log folder: ' + logFolder 
        
    if logBaseFileNames != None and getLogFileName != None:
        raise Exception('Specify only one of logBaseFileNames and getLogFileName')

    if logBaseFileNames != None and numTasks != len(logBaseFileNames):
        raise Exception('len(ids) != len(logBaseFileNames)')
    elif logBaseFileNames == None:
        logBaseFileNames = []
        for i in range(numTasks):
            logBaseFileNames.append(None)

    # We get the different involved ids. Each id has a list of the indexes 
    # (related to the original list)
    indexesPerId = {}
    for i in range(numTasks):
        identifier = ids[i]
        if identifier not in indexesPerId:
            indexesPerId[identifier] = []
        indexesPerId[identifier].append(i)
    
    childrenQueue = multiprocessing.Queue() # The queue of children tasks
    
    # we create as many tasks as ids 
    # (each task has the list of "whats" related to that id)
    numFinishedPerId = {}
    numPerId = {}
    for identifier in indexesPerId:
        numFinishedPerId[identifier] = 0
        numPerId[identifier] = len(indexesPerId[identifier])
        idWhats = []
        idLogBaseFileNames = []
        for index in indexesPerId[identifier]:
            idWhats.append(whats[index])
            idLogBaseFileNames.append(logBaseFileNames[index])
        childrenQueue.put([identifier, idWhats, idLogBaseFileNames])
    for i in range(maxChildren): #we add as many None jobs as numWorkers to tell them to terminate (queue is FIFO)
        childrenQueue.put(None)
    
    resultQueue = multiprocessing.Queue() # The queue where to put the results
    children = []
    # We start maxChildren children processes
    for i in range(maxChildren):
        children.append(multiprocessing.Process(target=runChild, 
            args=(i, childrenQueue, resultQueue, functionToApply, maxGrandChildren, logFolder, getLogFileName, dynamicLog)))
        children[-1].start()

    # print the results from the grandchildren
    resultsOk=[]
    resultsKo=[]
    numFinishedOk = 0
    numFinishedKo = 0
    for i in range(numTasks):
        if dynamicLog:
            logProgress(i, numTasks, numFinishedKo, numPerId, numFinishedPerId)        
        [resultType, identifier, result,] = resultQueue.get()
        if resultType == FINISHED_OK:
            numFinishedOk += 1
            resultsOk.append(result)
        else:
            numFinishedKo += 1
            resultsKo.append(result)
        
        numFinishedPerId[identifier] += 1
    if dynamicLog:
        logProgress(numTasks, numTasks, numFinishedKo, numPerId, numFinishedPerId)
        print # print empty to jump to next line
        
    # wait for all children to finish their execution
    for i in range(maxChildren):
        children[i].join()
        
    return (resultsOk, resultsKo)

# function run by each child
def runChild(childIndex, childrenQueue, resultQueue, functionToApply, maxGrandChildren, logFolder, getLogFileName, dynamicLog):
    tasksQueue = multiprocessing.Queue() # queue of tasks to be executed by the grandchildren
    childResultsQueue = multiprocessing.Queue() # queue of result from the grandchildren
    # We create the grandchildren
    tasksWorkers = []
    for i in range(maxGrandChildren): # we create the grandchildren
        tasksWorkers.append(multiprocessing.Process(target=runGrandChild, 
            args=(i, childIndex, tasksQueue, childResultsQueue, functionToApply, logFolder, getLogFileName, dynamicLog)))
        tasksWorkers[-1].start()
    
    kill_received = False
    while not kill_received:
        job = None
        try:
            # This call will patiently wait until new job is available
            job = childrenQueue.get()
        except:
            # if there is an error we will quit the loop
            kill_received = True
        if job == None:
            # If we receive a None job, it means we can stop the grandchildren too
            kill_received = True
        else:            
            #  job has a id and the "whats" related to that id
            [identifier, idWhats, idLogBaseFileNames] = job
            numIdTasks = len(idWhats)
            for i in range(numIdTasks):
                tasksQueue.put([i, identifier, idWhats[i], idLogBaseFileNames[i]])
            # Get the results of the grandchildren and transfer them to the parent
            for i in range(numIdTasks):
                resultQueue.put(childResultsQueue.get())
            
    for i in range(maxGrandChildren):
        tasksQueue.put(None)
        
    for i in range(maxGrandChildren):
        tasksWorkers[i].join()

# function run by each grandchild
def runGrandChild(grandChildIndex, childIndex, tasksQueue, resultQueue, functionToApply, logFolder, getLogFileName, dynamicLog):
    kill_received = False
    while not kill_received:
        job = None
        try:
            # This call will patiently wait until new job is available
            job = tasksQueue.get()
        except:
            # if there is an error we will quit the generation
            kill_received = True
        if job == None:
            # If we receive a None job, it means we can stop this workers 
            # (all the create-image jobs are done)
            kill_received = True
        else:            
            [taskIndex, identifier, what, logBaseFileName] = job
            try:
                sys.stdout.flush()
                sys.stderr.flush()
                if logFolder != None and logFolder != DEV_NULL:
                    if logBaseFileName != None:
                        logFilePath = logFolder + '/' + logBaseFileName + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'
                    elif getLogFileName != None:
                        logFilePath = logFolder + '/' + getLogFileName(identifier, what, childIndex, grandChildIndex, taskIndex)
                    else:
                        logFilePath = logFolder + '/' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'
                elif dynamicLog:
                    logFilePath = DEV_NULL
                else:
                    logFilePath = None
                if logFilePath != None:
                    sys.stdout = open(logFilePath,'w')
                    sys.stderr = sys.stdout
                result = functionToApply(identifier, what, childIndex, grandChildIndex, taskIndex)
                returnType = FINISHED_OK
            except Exception,e:
                result = e
                returnType = FINISHED_KO
            resultQueue.put([returnType, identifier, result])
