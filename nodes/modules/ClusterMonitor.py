from ledama import utils
import ledama.nodes.GetNodesUsage as GetNodesUsage
import ledama.nodes.GetNodesDiscs as GetNodesDiscs
import ledama.nodes.GetNodesNetStatus as GetNodesNetStatus
from ledama.utils import TARGET_E_EOR, TARGET_F_EOR
from ledama.PrettyTable import PrettyTable
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

class ClusterMonitor(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('nodes','n','Nodes to use',default='node001-080')
        options.add('uname','u','User name', helpmessage=', cpu and mem are only of the specified user',mandatory = False)
        
        # the information
        information = 'Command-line version of the Cluster Monitor. MAXCPU=1600. MAXMEM=100. DATA is the free space in each disk. The RX and TX are in MBps'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)
    
    def process(self, nodes, uname):
        if uname != '':
            self.userName = uname[:8]
        else:
            uname = None
        mountPoints = ['/data1', '/data2', '/data3']
        remoteMountPoints = ['eor',]
        nodes = utils.getNodes(nodes)
        remoteNodes = [TARGET_E_EOR, TARGET_F_EOR]
        commands = []
        users = []
        interfaceListNodeDict = dict( [(n, ['eth0',]) for n in nodes] )
        
        usageDict = GetNodesUsage.getUsageDictionary(nodes, users, commands, self.userName, None)
        discDict = GetNodesDiscs.getStorageDictionary(nodes, mountPoints)
        remoteDiscDict = GetNodesDiscs.getStorageDictionary(remoteNodes, remoteMountPoints, 86400)
        netDict = GetNodesNetStatus.getNetDictionary(interfaceListNodeDict)
        
        pTable = PrettyTable(('NODE','CPU','MEM','DATA1','DATA2','DATA3','RX','TX'))
        pTable.border = False

        for node in nodes:
            if uname == None:
                cpu = usageDict[node][1] # 2nd element is total cpu usage.
                mem = usageDict[node][3] # 3rd element is total mem usage.
            else:
                cpu = usageDict[node][0]
                mem = usageDict[node][2]
            (rx, tx) = netDict[(node, 'eth0')]
            disks = [] 
            for discUsage in discDict[node]:
                disks.append( discUsage ); 
            if cpu < 0:
                cpu = '*'
            if mem < 0:
                mem = '*'
            if rx < 0:
                rx = '*'
            else:
                rx = rx / (1024*1024)
            if tx < 0:
                tx = '*' 
            else:
                tx = tx / (1024*1024)
            pTable.add_row((node, str(cpu), str(mem), str(disks[0][1]), str(disks[1][1]), str(disks[2][1]), str(rx), str(tx) ))        
        print pTable.get_string()
        print 
        print
        pTable = PrettyTable(('TARGET','DATA'))
        pTable.border = False
        for node in remoteNodes:  
            pTable.add_row((node, str(remoteDiscDict[node][0][1])))
        print pTable.get_string()
        
