################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
import ledama.tasksdistributor as td
import ledama.utils as utils
from ledama.ReferenceFile import ReferenceFile
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

class DeleteData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 80)
        options.add('parentpath', 'd', 'Delete also parent path', default = False)
        options.add('force', 'f', 'Delete parent path even if not empty', default = False)
        
        # the information
        information = 'Delete data pointed by a refFile.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Function used for the tasksdistributor to delete a SB
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        (out,err) = td.execute("ssh " + node + " 'rm -r " + absPath + "'")
        if out != '':
            raise Exception((absPath, node, out))
        elif err != '':
            raise Exception((absPath, node, err))
        else:
            return (absPath, node)
    
    # Function used for the tasksdistributor to delete the parent paths
    def functionForParent(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        lsout = td.execute("ssh " + node + " 'ls " + absPath + "'")[0]
        if self.force or lsout == '':
            (rmout,rmerr) = td.execute("ssh " + node + " 'rm -r " + absPath + "'")
            if rmout != '':
                raise Exception((absPath, node, rmout))
            elif rmerr != '':
                raise Exception((absPath, node, rmerr))
        else:
            raise Exception((absPath, node, 'NOT EMPTY, delete manually: ' + lsout))
        return (absPath, node)
    
    def process(self, reffile, numprocessors, numnodes, parentpath, force):
        
        self.force = force
        
        referenceFile = ReferenceFile(reffile)
        
        # We delete each SB
        retValuesKo = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numprocessors, numnodes)[1]
        noCorrectDeleted = []
        for value in retValuesKo:
            if type(value) == Exception:
                noCorrectDeleted.append(' '.join(value.message))
          
        noCorrectParentDeleted = [] 
        if parentpath:
            # We also want to delete the parent path of each node
            sbsIndexesPerNode = utils.getIndexesDictionary(referenceFile.nodes)
            pnodes = []
            ppaths = []
            for node in sbsIndexesPerNode:
                parentPaths = set([])
                nodeIndexes = sbsIndexesPerNode[node]
                for index in nodeIndexes:
                    parentPaths.add(os.path.abspath(os.path.join(referenceFile.absPaths[index], '..')))
                for parentPath in parentPaths:
                    ppaths.append(parentPath)
                    pnodes.append(node)
            
            # Delete the parent paths in each node
            retPValuesKo = td.distribute(pnodes, ppaths, self.functionForParent, numprocessors, numnodes)[1]
            for pValue in retPValuesKo:
                if type(pValue) == Exception:
                    noCorrectParentDeleted.append(' '.join(pValue.message))      
        if len(noCorrectDeleted):
            print 'Num. errors in deleting MSPs: ' + str(len(noCorrectDeleted)) + '. Example of error message:'
            print '   ' + noCorrectDeleted[0]
        if len(noCorrectParentDeleted):
            print 'Num. errors in deleting parent paths in nodes: ' + str(len(noCorrectParentDeleted)) + '. Example of error message:'
            print '   ' + noCorrectParentDeleted[0]
        if not len(noCorrectDeleted) and not len(noCorrectParentDeleted):
            print 'All data in ' + reffile + ' correctly deleted!'