################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama import config as lconfig
from ledama.ReferenceFile import ReferenceFile
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

# File extensions
EXTENSION = '.vds'
EXPECTED_ERROR_MESSAGE = 'log4cplus:WARN Property configuration file "makevds.log_prop" not found.\nlog4cplus:WARN Using basic logging configuration.\n'
class CreateVDSFiles(LModule):    
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('output', 'o', 'Output VDS files folder', helpmessage='. This path should be visible and common for all the nodes, please write the absolute path')
        options.add('descfile', 'c', 'Cluster description file',helpmessage=' to be used for each makevds', default=utils.getDefClusDesc())
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'Create VDS files'
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Get the VDS file name (it is the MS name + .vds)
    def getVDSFileName(self, absPath):
        return msoperations.getMeasurementSetName(absPath) + EXTENSION
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        outputVdsFilePath =  self.outPath + '/' + self.getVDSFileName(absPath)
        # Define commands    
        commandMakevds = 'source ' + self.initfile + ' ; makevds ' + self.clustDescFilePath + ' ' + absPath + ' ' + outputVdsFilePath
        if node != currentNode:
            commandMakevds =  "ssh " + node + " '" + commandMakevds + "'"    
        (makevdsOut, makevdsErr) = td.execute(commandMakevds)
        if makevdsErr != '' and makevdsErr != EXPECTED_ERROR_MESSAGE:
            raise Exception(makevdsErr[:-1])
        elif makevdsOut != '':
            raise Exception(makevdsOut[:-1])
        return (absPath, outputVdsFilePath)
        
    def process(self, reffile, output, descfile, numprocessors, numnodes, initfile):
        self.initfile = os.path.abspath(initfile)
        self.outPath = os.path.abspath(utils.formatPath(output))
        self.clustDescFilePath = descfile
        referenceFile = ReferenceFile(reffile)
        # We create the directory
        os.system('mkdir -p ' + self.outPath)

        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numprocessors, numnodes)
        td.showKoFirst(retValuesKo) 
        if len(retValuesOk):
            print str(len(retValuesOk)) + ' VDS files were created. Check ' + self.outPath