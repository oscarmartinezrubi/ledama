################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import msoperations
from ledama import utils
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

class LaunchSagecal(LModule):
    
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('sky', 'k', 'Sky model file')
        options.add('cluster', 'c', 'Cluster file')
        options.add('tile', 't', 'Tile size', mandatory=False)
        options.add('solution', 'p', 'Solution Folder', mandatory=False, helpmessage=', solution files will be stored in this folder which must be visible from all the nodes')
        options.add('bandwidth', 'f', 'Bandwidth', mandatory=False, helpmessage=' MHz, for freq. smearing')
        options.add('options', 'o', 'Extra options', mandatory=False, helpmessage=', i.e. additional and advanced options to be used in the sagecal commands, use sagecal help to see them. If you are typing this in a terminal you should specify them between "", for example: "-n 8 -e 3 -g 2 -l 10 -m 7 -w 1 -b 1"')
        options.add('numprocessors', 'x', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('logspath', 'l', 'Logs path')
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = '/software/users/lofareor/eor-init.sh')
        # the information
        information = 'Launch sagecal'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
                
        # Define commands
        commandSagecal = 'source ' + self.initfile + ' ; sagecal -d ' + absPath + ' -s ' + self.skyFilePath + ' -c ' + self.clusterFilePath
        if self.tile != '':
            commandSagecal += " -t " + self.tile
        if self.bandwidth != '':
            commandSagecal += " -f " + self.bandwidth
        if self.solutionFolder != '':
            commandSagecal += " -p " + self.solutionFolder + '/' + msoperations.getMeasurementSetName(absPath) + '.sol'
        if self.options != '':
            # use the provided options but we remove possible ' and " used in the specification
            # this may happen if input arguments are specified by the web browser
            commandSagecal += " " + self.options.replace('"','').replace("'","")
        if node != currentNode:
            commandSagecal =  "ssh " + node + " '" + commandSagecal + "'"
        if self.query:
            return (absPath, commandSagecal)
        else:
            td.execute(commandSagecal, True) #we redirect output to log

    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'

    def process(self, reffile,sky, cluster, tile,solution,bandwidth, options,numprocessors, numnodes, logspath, query, initfile):
        self.query = query   
        self.initfile = os.path.abspath(initfile)
        self.skyFilePath = os.path.abspath(sky)
        self.clusterFilePath = os.path.abspath(cluster)
        self.tile = tile
        self.bandwidth = bandwidth
        self.options = options
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = os.path.abspath(logspath)
        
        self.solutionFolder = ""
        if solution != '':
            self.solutionFolder = os.path.abspath(solution)
            os.system("mkdir -p " + self.solutionFolder)
 
        referenceFile = ReferenceFile(reffile)
                
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, referenceFile.absPaths, 
            self.function, numprocessors, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 