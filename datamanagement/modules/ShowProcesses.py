################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import  utils
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

class ShowProcesses(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        # Define the options
        options = LModuleOptions()
        options.add('proc', 'p', 'Processes', helpmessage=' words that will filter out the processes to be shown. If multiple values separate them by commas.')
        options.add('kill', 'k', 'Kill the processes', helpmessage=', the processes are killed. It is recommended to detect the processes before using kill mode', default = False)
        options.add('nodes', 'u', 'Nodes', helpmessage=' where to search for the processes', default = 'node001-80')
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64 )
        # the information
        information = 'Show all the processes owned by current user which command name contains all the words given by the user. This also used to killed them'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    # Function used for the tasksdistributor to delete a SB
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        lines = []
        (out, err) = td.execute("ssh " + node + " 'ps aux " + self.allgrepstring + "'")
        if err != '':
            raise Exception(err[:-1])
        for line in out.split('\n'):
            if line != '' and not line.count(self.__class__.__name__): # let's exclude the commands that may be generated in current node
                selectable = True
                for grepoption in self.grepoptions:
                    if line.count(grepoption):
                        selectable = False
                if selectable:
                    if self.kill:
                        (out, err) = td.execute("ssh " + node + " 'kill -9 " + line.split()[1] + "'")
                        if err != '':
                            line += '\n      ' + err[:-1]
                        if out != '':
                            line += '\n      ' + out[:-1]
                    lines.append(line)
        return (node, lines)
    
    def process(self, proc, kill, nodes, numnodes):
        nodesToUse = utils.getNodes(nodes)
        self.kill = kill
        
        if len(self.userName) > 8:
            self.userName = os.popen('id').read().split()[0].replace('uid=','').replace('(' + self.userName + ')','')
        
        self.grepoptions = []
        self.grepoptions.append('grep ' + self.userName)
        for procfield in proc.split(','):
            self.grepoptions.append('grep ' + procfield)
            
        self.allgrepstring = ''
        for grepoption in self.grepoptions:
            self.allgrepstring += ' | ' + grepoption
            
        # We unpack each SB
        (retValuesOk,retValuesKo) = td.distribute(nodesToUse, nodesToUse, self.function, 1, numnodes)
        if len(retValuesOk):
            for (node, lines) in sorted(retValuesOk):
                print node
                for line in lines:
                    print '   ' + line
        td.showKoAll(retValuesKo)