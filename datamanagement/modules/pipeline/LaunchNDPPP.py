################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, re
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.datamanagement.modules.pipeline.CreateNDPPPParsetFiles import EXTENSION

class LaunchNDPPP(LModule):
    
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('parsets', 'i', 'Input NDPPP parset files folder')
        options.add('nodes', 'u', 'Nodes to run NDPPP', mandatory=False, helpmessage='. If not provided the nodes containing the data will be used')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)         
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)         
        options.add('logspath', 'l', 'Logs path')
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'Launch NDPPP'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Extract the node from a parset file, we suppose that the msin have this information
    # because we would have added the /net/node in the parset file creation
    def getNodeFromParset(self, parset):
        parsetFileLines = (open(parset, "r")).read().split('\n')
        for line in parsetFileLines:
            if line.startswith('msin=') or line.startswith('msin ='):
                f = re.search('node[0-9][0-9][0-9]', line)
                if f != None:
                    return (f.group(0))
                else:
                    raise Exception('Error getting node from ' + parset)
    
    def getMsInMsOut(self, parset):
        parsetFileLines = (open(parset, "r")).read().split('\n')
        msIn = None
        msOut = None
        for line in parsetFileLines:
            if line.startswith('msin=') or line.startswith('msin ='):
                msIn = line.split('=')[-1].strip()
            elif line.startswith('msout=') or line.startswith('msout ='):
                msOut = line.split('=')[-1].strip()
        return (msIn,msOut)

    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (parsetFile,msIn,msOut) = what
        currentNode = utils.getHostName()
        
        # Define commands
        commandNDPPP = 'source ' + self.initfile
        if msOut != '':
            commandNDPPP += " ; mkdir -p " + msoperations.getParentPath(msOut)
        commandNDPPP +=  ' ; NDPPP ' + parsetFile
        if node != currentNode:
            commandNDPPP =  "ssh " + node + " '" + commandNDPPP + "'"
        if self.query:
            return (msIn, commandNDPPP)
        else:
            td.execute(commandNDPPP, True) #we redirect output to log

    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (msIn,msOut) = what[1:]
        if msOut != '' and msOut != None:
            logFileName = msoperations.getMeasurementSetName(msOut)
        else:
            logFileName = msoperations.getMeasurementSetName(msIn)
        return logFileName + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'

    def process(self, parsets, nodes, numprocessors, numnodes, logspath, query, initfile):
        self.query = query   
        self.initfile = os.path.abspath(initfile)
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = os.path.abspath(logspath)

        # Get the parsets from the input path
        parsetFiles = []
        inputPath = os.path.abspath(parsets)
        if os.path.isdir(inputPath):
            for contentFile in sorted(os.listdir(inputPath), key=str.lower):
                if contentFile.endswith(EXTENSION): 
                    parsetFiles.append(inputPath + '/' + contentFile)
        if len(parsetFiles) == 0:
            print 'No parset files found in ' + inputPath
            return
        
        whats = []
        identifiers = []
        if nodes != '':
            nodesToUse = utils.getNodes(nodes)
            # we use the provided nodes
            parsetsForNodes = utils.splitArray(parsetFiles, len(nodesToUse))
            for i in range(len(nodesToUse)):
                node = nodesToUse[i]
                parsetsForNode = parsetsForNodes[i]
                for parset in parsetsForNode:
                    (msIn,msOut) = self.getMsInMsOut(parset)
                    whats.append((parset,msIn,msOut))
                    identifiers.append(node)
        else:
            # We use the nodes that contains the data (knowing that the parsets files)
            # should has as msin something like /net/nodeXXX
            for parset in parsetFiles:
                (msIn,msOut) = self.getMsInMsOut(parset)
                whats.append((parset,msIn,msOut))
                identifiers.append(self.getNodeFromParset(parset))
                
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(identifiers, whats, 
            self.function, numprocessors, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 