################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile
from ledama.datamanagement.modules.CreateRefFileFromPath import CreateRefFileFromPath

# File extensions
EXTENSION = '.bvf'
    
class ReadCorrXY(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile', helpmessage=' with the MS to convert to BVF')
        options.add('ofolder', 'o', 'Output BVF files folder',helpmessage='. The path of each output BVF will be: [output folder]/[MS name].bvf.')
        options.add('orefFile', 'r', 'Output BVF RefFile', mandatory=False)
        options.add('column', 'c', 'Column',helpmessage=', choose from DATA,CORR,MODEL')
        options.add('ignore', 'g', 'Ignore Stations', mandatory=False,helpmessage=', use indexes, for example 0,1,2')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 4)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = '/software/users/lofareor/eor-init.sh')      
        # the information
        information = 'Launch readcorrxy, i.e. it creates BVF files from the MS pointed by the reffile'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def getBVFFileName(self, absPath):
        return msoperations.getMeasurementSetName(absPath) + EXTENSION

    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        absPath = what
        node = identifier
        currentNode = utils.getHostName()
        commandRead = "source " + self.initfile + " ; mkdir -p " + self.outPath + " ; readcorrXY.py -m " + absPath + " -d " + self.outPath + '/' + self.getBVFFileName(absPath) + " -c " + self.column
        if self.ignore != '':
            commandRead += " -g " + self.ignore
        
        if node != currentNode:
            commandRead =  "ssh " + node + " '" + commandRead + "'"

        if self.query:
            return (absPath, commandRead)
        else:
            readerr = td.execute(commandRead)[1]
            if readerr != '':
                raise Exception(readerr[:-1])
            
    def process(self, reffile,ofolder,orefFile,column,ignore,numprocessors,numnodes,query,initfile):
        self.query = query
        self.initfile = os.path.abspath(initfile)
        self.column = column
        self.ignore = ignore
        self.outPath = utils.formatPath(ofolder)
        referenceFile = ReferenceFile(reffile)
        
        if orefFile != '':
            if os.path.isfile(orefFile):
                print 'Error: ' + orefFile + ' already exists'
                return
        
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numprocessors, numnodes)
        td.showOk(retValuesOk)
        td.showKoAll(retValuesKo) 
        if orefFile != '':
            if self.query:
                print 'No refFile is created in query mode.'
            else:
                CreateRefFileFromPath().process(self.outPath, ','.join(list(set(referenceFile.nodes))), orefFile, numprocessors, numnodes, True)