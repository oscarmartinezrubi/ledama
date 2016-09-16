################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama import tasksdistributor as td
from ledama.ReferenceFile import ReferenceFile

class UnPackData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 80)
        options.add('output', 'o', 'Output folder', mandatory=False, helpmessage=' (in each node) where the unpacked data is stored. If not provided, the same parent path of the input MS is used')
        options.add('mode', 'm', 'Tar mode', default = 'xvf')
        options.add('delete', 'd', 'Delete original', helpmessage = ' unpacked data', default = False)
        options.add('logspath', 'l', 'Logs path', mandatory = False)
        
        # the information
        information = 'Unpack (TAR) the data in a RefFile.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    # Function used for the tasksdistributor to delete a SB
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        if self.output == None:
            outputPath = msoperations.getParentPath(absPath)
        else:
            outputPath = self.output
        
        command = " tar " + self.mode + " " + absPath + " -C " + outputPath
        if self.delete:
            command += " ; rm -r " + absPath
        if node != currentNode:
            command =  'ssh ' + node + " '" + command + "'"
        if self.logsPath != None:
            td.execute(command, True) #we redirect output to log
        else:
            err = td.execute(command)[1]
            if err != '':
                raise Exception(err[:-1])
    
    def process(self, reffile, numprocessors, numnodes, output, mode, delete, logspath):
        self.output = None
        if output != '':
            self.output = os.path.abspath(output)
        self.mode = mode
        self.delete = delete
        self.logsPath = None
        if logspath != '':
            self.logsPath = os.path.abspath(logspath)
        referenceFile = ReferenceFile(reffile)
        retValuesKo = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numprocessors, numnodes, logFolder = self.logsPath,)[1]
        td.showKoAll(retValuesKo)