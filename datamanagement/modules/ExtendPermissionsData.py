################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

class ExtendPermissionsData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('user', 'u', 'User',)
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 80)
        
        # the information
        information = 'Extend writing permissions to all the MS in the reffile to the user (it also changes the parent path).'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
            
    # Function used for the tasksdistributor to change a SB
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        parentPath = what
        (chaclOut, chaclErr) = td.execute("ssh " + node + " 'chacl -r u::rwx,g::r-x,o::r-x,u:" + self.user + ":rwx,m::rwx " + parentPath +"'")
        if chaclErr != '':
            raise Exception(chaclErr[:-1])
        elif chaclOut != '':
            raise Exception(chaclOut[:-1])
        return (node, parentPath)
        
    def process(self, reffile, user, numprocessors, numnodes):
        self.user = user
        referenceFile = ReferenceFile(reffile)
        
        pnSet = set([])
        for i in range(len(referenceFile.absPaths)):
            pnSet.add((msoperations.getParentPath(referenceFile.absPaths[i]), referenceFile.nodes[i]))
        
        identifiers = []
        whats = []
        for (ppath,node) in pnSet:
            identifiers.append(node)
            whats.append(ppath)
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(identifiers, whats, self.function, numprocessors, numnodes)
        td.showKoFirst(retValuesKo)
        td.showOk(retValuesOk, True, 'Permissions were successfully changed in the following nodes and paths:')