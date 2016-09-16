################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

class LaunchAOFlagger(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('numthreads', 'p', 'Number of threads per MS', helpmessage=', in each node the MSs are flagged sequentially, but you can decide the number of threads in each MS flagging',default = 3)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('execpath', 'e', 'Execution path', helpmessage=', you must specify the path where the flagger is executed, it is not recommended to use the home directory as some temporal data is created')  
        options.add('strategy', 't', 'Strategy file path', mandatory=False)  
        options.add('logspath', 'l', 'Logs path')
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)        
        # the information
        information = 'Launch AOFlagger'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        # Define commands
        commandAOFlagger = 'source ' + self.initfile + ' ; mkdir -p ' + self.execpath + ' ; cd ' + self.execpath + " ; rficonsole -j " + str(self.numthreads) + " -indirect-read -v -nolog" 
        if self.strategy != '':
            commandAOFlagger += " -strategy " + self.strategy
        commandAOFlagger += " " + absPath
        if node != currentNode:
            commandAOFlagger =  'ssh ' + node + " '" + commandAOFlagger + "'"
        if self.query:
            return (absPath, commandAOFlagger)
        else:
            td.execute(commandAOFlagger, True) #we redirect output to log       

    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'

    def process(self, reffile, numthreads, numnodes, execpath, strategy, logspath, query, initfile):
        self.query = query   
        self.numthreads = numthreads
        self.initfile = os.path.abspath(initfile)
        self.execpath = utils.formatPath(execpath)
        self.strategy = strategy
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = utils.formatPath(os.path.abspath(logspath))
        referenceFile = ReferenceFile(reffile)
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, referenceFile.absPaths, 
            self.function, 1, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 