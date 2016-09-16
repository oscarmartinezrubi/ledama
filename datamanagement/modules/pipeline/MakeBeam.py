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

VALID_NODES = ['node079',]

class MakeBeam(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile',helpmessage=', it will be updated with the new sizes')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 32, helpmessage=', data is accessed via NFS')        
        # the information
        information = 'Make the beam tables, only work in node079'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Function used for the tasksdistributor to makebeam in a MS
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        # we create the path through NFS
        antennaSet = msoperations.getAntennaSet(absPath)
        if antennaSet != None:
            # we use NFS to access the MS (in this way we use local software in node079)
            makebeamerr = td.execute("makebeamtables  overwrite=true antennaset=" + antennaSet + " ms=" + '/net/' + node + absPath)[1]
            if makebeamerr != '':
                raise Exception(makebeamerr[:-1])

    def process(self, reffile, numprocessors, numnodes):
        if utils.getHostName() not in VALID_NODES:
            print 'Please, run this code in ' + str(VALID_NODES)
            return
        referenceFile = ReferenceFile(reffile)
        # Run it
        retValuesKo = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numprocessors, numnodes)[1]
        td.showKoAll(retValuesKo) 