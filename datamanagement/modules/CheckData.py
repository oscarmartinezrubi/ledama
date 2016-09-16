################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os,time
from ledama import tasksdistributor as td
from ledama import utils
from ledama import msoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile
from ledama.PrettyTable import PrettyTable

# Strings constants
CON_OK = 'CONNECTION_OK'
STATUS_AVAILABLE = "STATUS_AVAILABLE"
STATUS_UNKNOWN = "STATUS_UNKNOWN"
STATUS_COMPLETE = "COMPLETED"
STATUS_NOT_COMPLETE = "NOT_COMPLETED"
STATUS_BEING_TRANSFERRED = "BEING_TRANSFERRED"
DELAY = 15
CHECK_MOD_DELAY = 20

class CheckData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('inarg', 'i', 'Input', helpmessage = ' RefFile or status path')
        options.add('checkspeed', 'c', 'Check speed?', helpmessage = '. If the data is being copied you can get the actual speeds (this will slow down the check process). If the inarg is a status path, speeds are ALWAYS checked. ', default = False)
        options.add('notcompare', 'x', 'Skip size comparison?', helpmessage = ', it only shows observed size', default = False)
        options.add('acceptperc', 'a', 'Accepted percentage',helpmessage=' of data to consider a MS is completed when comparing the disk size with the refFile size (the size estimation in different machines may be different)', default = 100.)
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes',  default = 64)
        # the information
        information = 'Checks the availability of data pointed by RefFile by comparing the observed size with the one in the RefFile (also status path of a copy in progress can be used).'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (msAbsPath,msLogAbsPath, msLogTimeAbsPath, size) = what
        spentTime = 0
        obsSize = 0
        speed = 0
        status = STATUS_AVAILABLE
        isFDT = False
        msLogLines = []
        if msLogAbsPath != None and os.path.isfile(msLogAbsPath):
            msLogLines = open(msLogAbsPath).read().split('\n')
            for line in msLogLines:
                if line.count('INFO: FDT started in client mode'):
                    isFDT = True
                    break
            
        if isFDT:
            for line in msLogLines:
                if line.count('Net Out:') or line.count('Net In:'):
                    fields = line.split()
                    speed = float(fields[4]) / 8.
                    if len(fields) > 9:
                        obsSize = float(fields[9].replace('%','')) * size / 100.
                if line.count('FDT Session finished OK'):
                    obsSize = size
                    speed = 0
                    if msLogTimeAbsPath != None and os.path.isfile(msLogTimeAbsPath):
                        spentTime = float(open(msLogTimeAbsPath, "r").read())
                elif line.count('ERROR') or line.count('Exception'):
                    speed = 0
            if speed != 0:
                lastmod = os.path.getmtime(msLogAbsPath)
                current = time.time()
                if current > lastmod + CHECK_MOD_DELAY:
                    speed = 0
        else:
            if node.count('node') or node.count(utils.SOUTHAMPTON):
                if node.count(utils.SOUTHAMPTON):
                    node = 'as25g11@lofar1.phys.soton.ac.uk'
                sizeout = td.execute("ssh " + node + " 'echo " + CON_OK + "; du -sm " + msAbsPath + "'")[0]
                if sizeout.count(CON_OK) == 0:
                    status = STATUS_UNKNOWN
                else:
                    try:
                        obsSize = int((sizeout.split('\n')[1].split('\t'))[0])
                    except ValueError:
                        obsSize=0
                    if msLogTimeAbsPath != None and os.path.isfile(msLogTimeAbsPath):
                        spentTime = float(open(msLogTimeAbsPath, "r").read())
            else:
                status = STATUS_UNKNOWN

        return (msAbsPath, int(float(obsSize)), status, isFDT, float(speed), float(spentTime))    
    
    def getLogData(self, nodes, absPaths, sizes):
        """Use taskdistibutor to get the information for each MS"""
        childrens = []
        whats = []
        msLog = None
        msLogTime = None
        for i in range(len(absPaths)):
            childrens.append(nodes[i])
            if self.isTx:
                msName = msoperations.getMeasurementSetName(absPaths[i])
                msLog = self.statusPath + '/' + msName + '.log'
                msLogTime = self.statusPath + '/' + msName + '.time'
            whats.append((absPaths[i], msLog, msLogTime, sizes[i]))
        
        retValuesOk = td.distribute(childrens, whats, self.function, self.numprocessors, self.numnodes)[0]
        data = {}
        for retValue in retValuesOk:
            if type(retValue) == tuple:
                (msAbsPath, obsSize, status, isFDT, speed, spentTime) = retValue
                data[msAbsPath] = (obsSize, status, isFDT, speed, spentTime)
        return data
    
    # Returns a list of lines with the results of the analysis
    def showStatusResults(self, absPaths, inodes, onodes, sizes, status, percentages, times, speeds):
        if inodes != None:
            pTable = PrettyTable(('STATUS','path','inode','onode','size','percentage','time'))
        else:
            pTable = PrettyTable(('STATUS','path','onode','size','percentage','time'))
        pTable.border = False
        statusCounter = {}
        sizesPerStatus = {}
        timesCompleted = 0.
        
        # Add the table of the status
        for statusCase in (STATUS_COMPLETE, STATUS_UNKNOWN, STATUS_NOT_COMPLETE, STATUS_BEING_TRANSFERRED):
            statusCounter[statusCase] = 0
            sizesPerStatus[statusCase] = 0
            for i in range(len(absPaths)):
                if status[i] == statusCase:
                    if status[i] == STATUS_COMPLETE:
                        timesCompleted += times[i]
                        sizesPerStatus[statusCase] += sizes[i]
                    else:
                        sizesPerStatus[statusCase] += sizes[i] * (percentages[i]/100.)
                    statusCounter[statusCase] += 1
                    if inodes != None:
                        pTable.add_row((statusCase, absPaths[i], inodes[i], onodes[i], str(sizes[i]), str(percentages[i]), str(times[i])))
                    else:
                        pTable.add_row((statusCase, absPaths[i], onodes[i], str(sizes[i]), str(percentages[i]), str(times[i]) ))
        print pTable.get_string()
        
        totalSize = sum(sizes)
        completePercentage = 0
        completeSize = 0
        if totalSize > 0:
            completeSize = sizesPerStatus[STATUS_COMPLETE] + sizesPerStatus[STATUS_NOT_COMPLETE] + sizesPerStatus[STATUS_BEING_TRANSFERRED]
            completePercentage = completeSize * 100./totalSize
    
        # We add some information about completed tx (and volume of data)
        print
        print '# ' + str(statusCounter[STATUS_COMPLETE]) + ' / ' + str(len(absPaths)) + ' SBs, ' +  str(completeSize) + ' / ' + str(totalSize) +  ' MB -> ' + str(completePercentage) + ' %'
        
        averageSpeedInMBpsPerSB = 0
        if timesCompleted != 0:
            averageSpeedInMBpsPerSB = float(sizesPerStatus[STATUS_COMPLETE]) / timesCompleted
            print '# Average observed speed per Band: ' + ("%.3f" % (averageSpeedInMBpsPerSB*8.)) + ' Mbps'

        if statusCounter[STATUS_BEING_TRANSFERRED]:
            print '# Num. simultaneous copies: ' + str(statusCounter[STATUS_BEING_TRANSFERRED])

        # Sum the given speed to get a total current speed
        estimatedCurrentSpeed = 0
        for i in range(len(speeds)):
            if speeds[i] > 0.:
                estimatedCurrentSpeed += speeds[i]
        if estimatedCurrentSpeed > 0:
            estimatedCurrentSpeedMbps = estimatedCurrentSpeed*8.
            if estimatedCurrentSpeedMbps > 1024.:
                print '# ' + 'Current estimated total transfer speed: ' + ("%.3f" % (estimatedCurrentSpeedMbps/1024.)) + ' Gbps'
            else:
                print '# ' + 'Current estimated total transfer speed: ' + ("%.3f" % estimatedCurrentSpeedMbps) + ' Mbps'
        else:
            # If the copy does not involve Target we do not have speed info, speed will be < 0
            estimatedCurrentSpeed = averageSpeedInMBpsPerSB * statusCounter[STATUS_BEING_TRANSFERRED] 
            
        if estimatedCurrentSpeed > 0:
            expectedRemainingTime = float(totalSize-completeSize) / estimatedCurrentSpeed
            if expectedRemainingTime > 0:
                print '# ' + 'Estimated remaining time: ' + utils.getInHMS(int(expectedRemainingTime))
        print 
    
    def process(self, inarg, checkspeed, notcompare, acceptperc, numprocessors, numnodes):
        self.numprocessors = numprocessors
        self.numnodes = numnodes
        irefFile = ''
        orefFile = ''
        # if input is a dir, we are treating a status path
        if os.path.isdir(inarg):
            # It is the status path
            self.statusPath = utils.formatPath(inarg)
            # refFile should be found inside staus path
            for f in os.listdir(self.statusPath):
                if f.endswith(ReferenceFile.EXTENSION):
                    if f.startswith('IN'):
                        irefFile = self.statusPath + '/' + f
                    else:
                        orefFile = self.statusPath + '/' + f
            
            if orefFile == '':
                print 'Error: Reference file not found in ' + self.statusPath + '. Please, if you are transferring the LOFARDataSet, place the new reference file in ' + self.statusPath
                return
        else:
            # It is only the refFile
            orefFile = inarg
            self.statusPath = ''
        
        # Check if the reffile exists
        if not os.path.isfile(orefFile):
            print 'Error: ' + orefFile + ' does not exist'
            return
        
        statuss = []
        times = []
        percentages = []
        speeds = []
        
        # Create the ReferenceFile object
        oreferenceFile = ReferenceFile(orefFile)
        onodes = oreferenceFile.nodes
        oabsPaths = oreferenceFile.absPaths
        osizes = oreferenceFile.sizes
        
        oreferenceFile.validateSizes()
        
        inodes=None
        if irefFile != '':
            inodes =  ReferenceFile(irefFile).nodes
        
        checkTwice = (checkspeed or self.statusPath != '')
        self.isTx = self.statusPath != ''
        obsData = self.getLogData(onodes, oabsPaths, osizes)
        
        if notcompare:
            for i in range(len(oabsPaths)):
                size = obsData[oabsPaths[i]][0] # size is first element
                print oabsPaths[i] + '\t' + onodes[i] + '\t' + str(size)
        else:
            oabsPaths2 = []
            onodes2 = []
            osizes2 = []
            areFDT = False
            for i in range(len(oabsPaths)):
                msAbsPath = oabsPaths[i]
                if msAbsPath not in obsData:
                    statuss.append(STATUS_UNKNOWN)
                    percentages.append(0.)
                    times.append(0.)
                    speeds.append(0.)
                else:
                    (size, status, isFDT, speed, spentTime) = obsData[msAbsPath]
                    if isFDT:
                        areFDT = True
                    if status == STATUS_UNKNOWN:
                        statuss.append(STATUS_UNKNOWN)
                        percentages.append(0.)
                    elif size < (osizes[i] * (float(acceptperc)/100.)):
                        percentages.append(size * 100./osizes[i])
                        if speed > 0.:
                            statuss.append(STATUS_BEING_TRANSFERRED)
                        else:
                            statuss.append(STATUS_NOT_COMPLETE)
                            if not areFDT and checkTwice:
                                oabsPaths2.append(msAbsPath)
                                onodes2.append(onodes[i])   
                                osizes2.append(osizes[i])
                    else:
                        statuss.append(STATUS_COMPLETE)
                        percentages.append(100.)
                    times.append(spentTime)
                    speeds.append(speed)
                
            if len(oabsPaths2):
                # We get a second sampling of the sizes in order to see which 
                # SBs have changed its sizes. Those will be the ones currently tx
                # Give some seconds for the transfers to advance, if we do not this we 
                # do not detect which SBs are being transferred (sizes are changed)
                print 'Sleeping for ' + str(DELAY) + 's...'
                os.system('sleep ' + str(DELAY))
                obsData2 = self.getLogData(onodes2, oabsPaths2, osizes2)
                for i in range(len(oabsPaths)):
                    msAbsPath = oabsPaths[i]
                    if msAbsPath in obsData2:
                        size = obsData[msAbsPath][0]
                        size2 = obsData2[msAbsPath][0]
                        if size2 > size:
                            statuss[i] = STATUS_BEING_TRANSFERRED
                            speeds[i] = float(size2 - size) / float(DELAY)
                        else:
                            speeds[i] = 0.
            # Show the resutls
            self.showStatusResults(oabsPaths, inodes, onodes, osizes, statuss, percentages, times, speeds)
