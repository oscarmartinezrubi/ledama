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
    
class WriteCorrXY(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('inputbvf', 'b', 'Input BVF RefFile')
        options.add('inputms', 'm', 'Input MS RefFile',helpmessage=' where the data will be written')
        options.add('column', 'c', 'Column',helpmessage=', choose from CORR,MODEL')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 4)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = '/software/users/lofareor/eor-init.sh')      

        # the information
        information = 'Launch writecorrxy, i.e. it updates the column of the MSs pointed by the reffile with the related BVF file'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (bvfNode, bvfAbsPath, absPath) = what
        currentNode = utils.getHostName()
        
        commandWrite = "source " + self.initfile + " ; writecorrXY.py -m " + absPath  + " -c " + self.column
        if node != bvfNode:
            commandWrite +=  " -d /net/" + bvfNode + bvfAbsPath
        else:
            commandWrite +=  " -d " + bvfAbsPath
            
        if node != currentNode:
            commandWrite =  "ssh " + node + " '" + commandWrite + "'"
    
        if self.query:
            return (absPath, commandWrite)
        else:
            readerr = td.execute(commandWrite)[1]
            if readerr != '':
                raise Exception(readerr[:-1])
            
    def process(self, inputbvf,inputms,column,numprocessors,numnodes,query,initfile):
        self.query = query
        self.column = column
        self.initfile = os.path.abspath(initfile)
        referenceFileBVF = ReferenceFile(inputbvf)
        referenceFileMS = ReferenceFile(inputms)
        
        # Dictionary of indexes
        msDict = {}
        for i in range(len(referenceFileMS.absPaths)):
            msAbsPath = referenceFileMS.absPaths[i]
            msIndex = msoperations.getSBIndex(msAbsPath)
            if msIndex in msDict:
                print 'Error matching indexes: Input MS file contains duplicate SB%03d' % (msIndex)
                return
            msDict[msIndex] = (msAbsPath, referenceFileMS.nodes[i])
        
        nodes = []
        whats = []
        bvfDict = {}
        for i in range(len(referenceFileBVF.absPaths)):
            bvfAbsPath = referenceFileBVF.absPaths[i]
            bvfIndex = msoperations.getSBIndex(bvfAbsPath)
            if bvfIndex in bvfDict:
                print 'Error matching indexes: Input BVF file contains duplicate SB%03d' % (bvfIndex)
                return
            if bvfIndex not in msDict:
                print 'Warning: SB%03d is in BVF refFile but not in MS refFile' % (bvfIndex)
            else:
                bvfDict[bvfIndex] = bvfAbsPath
                (msAbsPath,msNode) = msDict[bvfIndex]
                nodes.append(msNode)
                whats.append((referenceFileBVF.nodes[i], bvfAbsPath, msAbsPath))

        # Run it
        (retValuesOk, retValuesKo) = td.distribute(nodes, whats, self.function, numprocessors, numnodes)
        td.showOk(retValuesOk)
        td.showKoAll(retValuesKo) 