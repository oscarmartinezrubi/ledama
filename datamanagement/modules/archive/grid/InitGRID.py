################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
                    
class InitGRID(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('nodes','u','Nodes to initialize GRID',helpmessage=', these nodes must have the GRID software installed',default='node011-074')
        options.add('passphrase','p','Passphrase',default='lofardata')
        options.add('vomsconf','v','Voms option',helpmessage=' to be used in the voms-proxy-init. Usual values are lofar:/lofar/eor or lofar:/lofar/user',default='lofar:/lofar/user')
        options.add('gridinitfile', 's', 'GRID init. file', helpmessage = ', this file is "sourced" in each remote node before voms initialization', default = lconfig.GRID_INIT_FILE)
        
        # the information
        information = 'Initialize the GRID software in the specified nodes.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self, nodes, passphrase, vomsconf, gridinitfile):

        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        
        # We prepare a list of nodes-paths to be checked in each node
        for node in utils.getNodes(nodes):
            print 'Initializing ' + node
            os.system('ssh ' + node + ' " source ' + gridinitfile + '; echo ' + passphrase + ' | voms-proxy-init -valid 96:00 -voms ' + vomsconf + ' -pwstdin"')