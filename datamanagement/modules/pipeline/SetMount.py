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
from ledama.ReferenceFile import ReferenceFile

def functionForSSH(absPath):
    os.system("taql 'update " + absPath +"/ANTENNA" """ set MOUNT = "ALT-AZ"'""")

class SetMount(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile',helpmessage=', it will be updated with the new sizes')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'Set MOUNT to data pointed by a RefFile'
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        commandSetmount = 'source ' + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + functionForSSH.__name__ +'(\\\"' + absPath +'\\\")"'
        if node != currentNode:
            commandSetmount =  "ssh " + node + " '" + commandSetmount + "'"
        unflagerr = td.execute(commandSetmount)[1]
        if unflagerr != '':
            raise Exception(unflagerr[:-1])

    def process(self, reffile, numprocessors, numnodes,initfile):
        self.initfile = os.path.abspath(initfile)
        referenceFile = ReferenceFile(reffile)
        # Run it
        retValuesKo = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numprocessors, numnodes)[1]
        td.showKoAll(retValuesKo) 