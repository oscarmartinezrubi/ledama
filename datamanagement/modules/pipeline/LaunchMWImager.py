################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import tasksdistributor as td
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.datamanagement.modules.pipeline.CreateMWImagerParsetFiles import EXTENSION

class LaunchMWImager(LModule):
    
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('parsets', 'i', 'Input MWImager parset files folder')
        options.add('numprocessors', 'p', 'Simultaneous MWImager executions', default = 10)         
        options.add('logspath', 'l', 'Logs path')
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)        
        # the information
        information = 'Launch MWImager'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):  
        parsetFile = what
        commandMwimager = "source " + self.initfile + " ; mwimager " + parsetFile
        if self.query:
            return (parsetFile, commandMwimager)
        else:
            td.execute(commandMwimager, True) #we redirect output to log       

    def process(self, parsets, logspath, numprocessors, query, initfile):
        self.initfile = os.path.abspath(initfile)
        self.query = query
        currentNode = utils.getHostName()
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = os.path.abspath(logspath)
        # Get the parset files from the input path
        inputPath = utils.formatPath(parsets)
        whats = []
        identifiers = []
        if os.path.isdir(inputPath):
            for pfile in sorted(os.listdir(inputPath), key=str.lower):
                if pfile.endswith(EXTENSION): 
                    whats.append(inputPath + '/' + pfile)
                    identifiers.append(currentNode)
            
        if len(whats) == 0:
            print 'No parset files found in ' + inputPath
            return
        
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(identifiers, whats, 
            self.function, numprocessors, 1, logFolder = logsPath)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 