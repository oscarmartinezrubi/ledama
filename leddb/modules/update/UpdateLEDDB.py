import re, os, time, random
from ledama import utils
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.PrettyTable import PrettyTable
from ledama.leddb import LEDDBOps
from ledama.leddb.Naming import HOST, REQUIRESUPDATE, NAME

class UpdateLEDDB(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('path','i','Path[s]',default='/data1/users/lofareor,/data1/users/lofareor/pipeline,/data1/users/lofareor/backup,/data1/users/lofareor/backup/pipeline,/data2/users/lofareor,/data2/users/lofareor/pipeline,/data2/users/lofareor/backup,/data2/users/lofareor/backup/pipeline,/data3/users/lofareor,/data3/users/lofareor/pipeline,/data3/users/lofareor/backup,/data3/users/lofareor/backup/pipeline', helpmessage=' (if multiple, comma-separated no blank spaces), for each one of the provided paths we will list only LDS paths, i.e. the sub-paths L*')
        options.add('nodes','s','Nodes to update:',default='node011-074')
        options.add('usedb','u','Use the LEDDB to get the paths to check',default=False, helpmessage='. If selected the paths indicated with -i will be ignored.')
        options.add('notclean','c','Do not clean the MSP records in the LEDDB',default=False)
        options.add('diagnostic','d','Update also the diagnostic data',default=False)
        options.add('setcron','a','Set Cron task ?',default=False, helpmessage='. It will set the REQUIRE_UPDATE=True in all the selected nodes. This is used together with the cron jobs set in the nodes')
        options.add('runcron','f','Cron task ?',default=False, helpmessage='. It will only update the nodes if a cron task has been previously setup')
        options.add('processused','g','Process MS being used',default=False, helpmessage='(this may cause threads get stuck in table locks)')
        options.add('numprocessors','p','Simultaneous processors per node',default=1)
        options.add('numnodes','n','Simultaneous nodes',default=15)
        options.add('logs','l','Logs path',default=utils.getHome(userName) + '/logsLEDDB')
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)
        options.add('showstatus','t','Show status of last update',default=False)  
        options.add('initfile', 'e', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'update the LEDDB'
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   
  
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        attributes = ' -i ' + self.path
        if self.usedb:
            attributes += ' -u'
        if self.notclean:
            attributes += ' -c'
        if self.diagnostic:
            attributes += ' -d'
        if self.cron:
            attributes += ' -f'
        if self.processused:
            attributes += ' -g'
        attributes += ' -p ' + str(self.numprocessors)
        attributes += ' -w ' + self.dbname
        attributes += ' -y ' + self.dbuser
        attributes += ' -z ' + self.dbhost
        time.sleep(random.randrange(0, 20))
        td.execute("ssh " + node + " 'source " + self.initfile + " ; python " + utils.SOFTWAREPATH + "ledama/ExecuteLModule UpdateLEDDBNode " + attributes + "'", True)
    
    def getTotalUpdateStats(self, nodes):
        totals = 0
        errors = 0
        news = 0
        deleteds = 0
        updatedgains = 0
        updatedqualities = 0
        pTable = PrettyTable(['node', 'finishedTime', 'spentTime', '#MSPs', '#Added', '#Errors', '#GainsUp', '#QualitiesUp', '#Deleted'])
        times = []
        setnodes=set(nodes)
        results = os.popen('cat ' + self.logsPath + '/node*.log | grep STATS').read().split('\n')
        for line in results:
            try:
                if line.count('node'):
                    node = re.search('node[0-9][0-9][0-9]', line).group(0)
                    if node in setnodes:
                        setnodes.remove(node)
                        time = int(re.search('[0-9]* seconds', line).group(0).replace(' seconds',''))
                        times.append(time)
                        total = int(re.search('Total processed MSPs: [0-9]*', line).group(0).replace('Total processed MSPs: ',''))
                        totals += total
                        error = int(re.search('Errors: [0-9]*', line).group(0).replace('Errors: ',''))
                        errors += error
                        newmsp = int(re.search('Added: [0-9]*', line).group(0).replace('Added: ',''))
                        news += newmsp
                        deleted = int(re.search('Deleted: [0-9]*', line).group(0).replace('Deleted: ',''))
                        deleteds += deleted
                        updatedgain = int(re.search('Updated Gains: [0-9]*', line).group(0).replace('Updated Gains: ',''))
                        updatedgains += updatedgain
                        updatedquality = int(re.search('Updated Quality: [0-9]*', line).group(0).replace('Updated Quality: ',''))
                        updatedqualities += updatedquality
                        pTable.add_row([node,line.split('-')[0],('%d'%time),('%d'%total),('%d'%newmsp),('%d'%error),('%d'%updatedgain),('%d'%updatedquality),('%d'%deleted)])
            except:
                continue
            
        averagetime = 0
        if len(times):
            averagetime = sum(times)/len(times)
        pTable.add_row(['TOTAL','XXXX/XX/XX/XX:XX:XX',('%d'%averagetime),('%d'%totals),('%d'%news),('%d'%errors),('%d'%updatedgains),('%d'%updatedqualities),('%d'%deleteds)])  
        print pTable.get_string()
            
    def getUnfinishedNodes(self, nodes):
        setnodes=set(nodes)
        results = os.popen('cat ' + self.logsPath + '/node*.log | grep finished').read().split('\n')
        for line in results:
            try:
                if line.count('node'):
                    node = re.search('node[0-9][0-9][0-9]', line).group(0)
                    if node in setnodes:
                        setnodes.remove(node)
            except:
                continue
            
        incompletedNodes = sorted(list(setnodes))
        nodesCompletedCounter = len(nodes) - len(incompletedNodes)
        auxString = ''
        for incompleteNode in incompletedNodes:
            auxString += incompleteNode + ' '
        if auxString != '':
            print str(nodesCompletedCounter) + ' of ' + str(len(nodes)) + ' nodes have finished. Incomplete update in: ' + auxString
        else:
            print str(nodesCompletedCounter) + ' of ' + str(len(nodes)) + ' nodes have finished.'
        
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        logFile = identifier + '.log'
        logFileAbsPath = self.logsPath + '/' + logFile
        if os.path.isfile(logFileAbsPath):
            os.system('rm ' + logFileAbsPath)
        return logFile
    
    def process(self,path,nodes,usedb,notclean,diagnostic,setcron, runcron,processused,numprocessors,numnodes,logs,dbname,dbuser,dbhost,showstatus,initfile): 
        self.initfile = os.path.abspath(initfile)
        self.diagnostic = diagnostic
        self.path = path
        self.usedb = usedb
        self.notclean = notclean
        self.cron = runcron
        self.numprocessors = numprocessors
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.processused = processused
        self.logsPath = None
        if logs != '':
            self.logsPath = os.path.abspath(logs)
        nodesToUse = utils.getNodes(nodes)
        if showstatus:
            # Show status of last performes LEDDB Update and exit
            self.getUnfinishedNodes(nodesToUse)
            self.getTotalUpdateStats(nodesToUse)
            return
        
        if setcron:
            connection = Connector(dbname, dbuser, dbhost).getConnection()
            for node in nodesToUse:
                LEDDBOps.update(connection, HOST, {REQUIRESUPDATE:True,}, {NAME:node,})
            connection.close()
            return
        
        # Make the update in all the nodesToUse    
        tstart = time.time()
        print utils.getCurrentTimeStamp() + ' - Updating LEDDB...'
        retValuesKo = td.distribute(nodesToUse, nodesToUse, self.function, 1, numnodes, logFolder = self.logsPath, getLogFileName = self.getLogFileName)[1]
        td.showKoAll(retValuesKo)
        print utils.getCurrentTimeStamp() + ' - Finished update of LEDDB (' + str(time.time() - tstart) + ' seconds)'
        self.getUnfinishedNodes(nodesToUse)
        self.getTotalUpdateStats(nodesToUse)