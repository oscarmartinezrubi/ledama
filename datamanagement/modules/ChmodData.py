################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import tasksdistributor as td
from ledama import utils
from ledama import msoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

class ChmodData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('parentpath', 'c', 'Change also parent path', default = False)
        options.add('mode', 'm', 'Mode used in chmod', default = '775')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes',  default = 64)
        
        # the information
        information = 'Runs chmod to the data pointed by RefFile.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
            
    # Function used for the tasksdistributor to change a SB
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        (out,err) = td.execute("ssh " + node + " 'chmod -R " + self.mode + " " + absPath + "'")
        if out != '':
            raise Exception(out[:-1])
        elif err != '':
            raise Exception(err[:-1])
        else:
            return (node, absPath)    
    
    def process(self, reffile, parentpath, mode, numprocessors, numnodes):
        self.mode = mode
        referenceFile = ReferenceFile(reffile)
        identifiers = []
        whats = []
        if not parentpath:
            identifiers = referenceFile.nodes
            whats = referenceFile.absPaths
        else:
            indexesPerNode = utils.getIndexesDictionary(referenceFile.nodes)
            for node in indexesPerNode:
                parentPaths = set([])
                for index in indexesPerNode[node]:
                    parentPaths.add(msoperations.getParentPath(referenceFile.absPaths[index]))
                for parentPath in parentPaths:
                    identifiers.append(node)
                    whats.append(parentPath)
                
        (retValuesOk,retValuesKo) = td.distribute(identifiers, whats, self.function, numprocessors, numnodes)
        td.showOk(retValuesOk, True)
        td.showKoFirst(retValuesKo)