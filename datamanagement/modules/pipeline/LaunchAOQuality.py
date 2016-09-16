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
    
class LaunchAOQuality(LModule):
    
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('aoqualitycommand', 'c', 'Command to be used in AOQuality', helpmessage=', you must specify the path where the flagger is executed, it is not recommended to use the home directory as some temporal data is created')  
        options.add('logspath', 'l', 'Logs path')
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)        
        # the information
        information = 'Launch AOQuality'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        # Define commands
        commandAOQuality = 'source ' + self.initfile + " ; aoquality " + self.aoqualitycommand + " " + absPath
        if node != currentNode:
            commandAOQuality =  'ssh ' + node + " '" + commandAOQuality + "'"
        if self.query:
            return (absPath, commandAOQuality)
        else:
            td.execute(commandAOQuality, True) #we redirect output to log       
    
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'

    def process(self, reffile, numprocessors, numnodes, aoqualitycommand, logspath, query, initfile):
        self.query = query   
        self.initfile = os.path.abspath(initfile)
        self.aoqualitycommand = aoqualitycommand
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = utils.formatPath(os.path.abspath(logspath))
        referenceFile = ReferenceFile(reffile)
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, referenceFile.absPaths, 
            self.function, numprocessors, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 