################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, sys
from ledama import utils
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST
from ledama.leddb.Naming import GAIN, QTSTAT, QBSTAT, QFSTAT, ALLTABLES
from ledama.leddb.modules.CreateLEDDB import CreateLEDDB

class RestoreLEDDB(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)     
        options.add('backupdir', 'b', 'Backup directory where to find the backup tables')     
        options.add('functions','f','Create also the functions (using CreateLEDDB)?',default=False)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        
        # the information
        information = 'restore a LEDDB'
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def runCommand(self, command, query):
        print command
        if not query:
            sys.stdout.flush()
            os.system(command)
    
    def process(self, dbname, dbuser, dbhost, backupdir, functions, query):
        if not os.path.isdir(backupdir):
            print 'ERROR: ' + backupdir + ' is not found' 
        if query:
            print 'cd ' + backupdir
        else:
            os.chdir(backupdir)
        # We make a restore of all the LEDDB except the partitions of the DIAG tables
        for table in ALLTABLES:
            self.runCommand('gunzip -c ' + table.lower() + '.tgz | psql -d ' + dbname + ' -h ' + dbhost + ' -U ' + dbuser, query)
        
        for diagTable in (GAIN, QTSTAT, QBSTAT, QFSTAT):
            somePartition = False
            partitions = []
            for bTable in os.listdir('.'):
                if bTable.count(diagTable.lower()+"_part_"):
                    somePartition = True
                    partitions.append(bTable)
            for partition in sorted(partitions):
                self.runCommand('gunzip -c ' + partition + ' | psql -d ' + dbname + ' -h ' + dbhost + ' -U ' + dbuser, query)
            if somePartition:
                self.runCommand('gunzip -c ' + diagTable.lower() + '.tgz | psql -d ' + dbname + ' -h ' + dbhost + ' -U ' + dbuser, query)
                            
        # Now we recreate all the functions
        if functions:
            if query:
                print ' # In query mode the functions are not created!'
            else:
                CreateLEDDB().process(dbname, dbuser, dbhost, True)