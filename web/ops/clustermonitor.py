import json
import cherrypy
from cherrypy.lib import httpauth
from ledama import utils
import ledama.nodes.GetNodesUsage as GetNodesUsage
import ledama.nodes.GetNodesDiscs as GetNodesDiscs
import ledama.nodes.GetNodesNetStatus as GetNodesNetStatus
from ledama.utils import TARGET_E_EOR, TARGET_F_EOR

class ClusterMonitor:
    @cherrypy.expose
    def default(self, node = None, uname = None, cmd = None, type = None):
        mountPoints = ['/data1', '/data2', '/data3']
        remoteMountPoints = ['eor',]
        pathsToCheck = ['/data1/users/lofareor', '/data2/users/lofareor', '/data3/users/lofareor']
        if node and type == "cpu":
            lines = GetNodesUsage.getUsageSnapshot(node, 'CPU', '')
            obj = {}
            obj['header'] = lines[6].split()
            obj['data'] = list( [ line.split() for line in lines[8:-2] ] )
            obj['extra'] = list( lines[:5] )
            return json.dumps(obj)
        elif node and type == "disk":
            obj = {}
            obj['header'] = ['MountPoint', 'PATH', 'SIZE']
            obj['data'] = []
            for i in range(len(mountPoints)):
                mountpoint = mountPoints[i]
                (paths,sizes) = GetNodesDiscs.getPathContent(node, pathsToCheck[i])
                for i in range(len(paths)):
                    obj['data'].append([mountpoint, paths[i], sizes[i]])
            return json.dumps(obj)
        else:
            nodes = utils.getNodes('node001-80')
            remoteNodes = [TARGET_E_EOR, TARGET_F_EOR]
            commands = []
            users = []
            interfaceListNodeDict = dict( [(n, ['eth0',]) for n in nodes] )
            if str(uname).strip() == '':
                uname = None
            if str(cmd).strip() == '':
                cmd = None
            username = uname
            if uname == None:
                # If none user name is provided we use the logged user. This will only affect to the commands thar are returned
                ah = httpauth.parseAuthorization(cherrypy.request.headers['authorization']) 
                username = ah['username']
            usageDict = GetNodesUsage.getUsageDictionary(nodes, users, commands, username, cmd)
            discDict = GetNodesDiscs.getStorageDictionary(nodes, mountPoints)
            remoteDiscDict = GetNodesDiscs.getStorageDictionary(remoteNodes, remoteMountPoints, 86400)
            netDict = GetNodesNetStatus.getNetDictionary(interfaceListNodeDict)
            NodeStats = {}
            NodeStats['nodes'] = []
            NodeStats['users'] = users
            NodeStats['cmds'] = commands
            for node in nodes:
                obj = {}
                obj['nodeid'] = node 
                if uname == None:
                    obj['cpu'] = usageDict[node][1] # 2nd element is total cpu usage.
                    obj['mem'] = usageDict[node][3] # 3rd element is total mem usage.
                else:
                    obj['cpu'] = usageDict[node][0]
                    obj['mem'] = usageDict[node][2]
                (rx, tx) = netDict[(node, 'eth0')]
                obj['net'] = [ ['eth0', rx, tx],  ]
                obj['disks'] = []
                for discUsage in discDict[node]:
                    obj['disks'].append( discUsage );  
                NodeStats['nodes'].append( obj )
            
            for node in remoteNodes:
                obj = {}
                obj['nodeid'] = node
                obj['disks'] = []
                for discUsage in remoteDiscDict[node]:
                    obj['disks'].append( discUsage );  
                NodeStats['nodes'].append( obj )
            return json.dumps(NodeStats)
