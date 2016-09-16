################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, sys
from ledama import utils
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import Connector, DEF_DBNAME, DEF_DBHOST
from ledama.leddb.Naming import GAIN, QTSTAT, QBSTAT, QFSTAT, LASTCHECK, SEQ, ALLTABLES

class BackupLEDDB(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)
        options.add('backup','b','Backup?',default=False)     
        options.add('backupdir', 'd', 'Backup directory (in current node) where all the tables will be dumped', default='/data3/users/lofareor/leddbbackup')
        options.add('vacuum','v','Vacuum?',default=False)
        options.add('full','f','Full vacuum?',default=False)
        options.add('reindex','r','Reindex?',default=False)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        
        # the information
        information = 'backup, vacuum and reindex a LEDDB'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def runCommand(self, command, query):
        print command
        if not query:
            sys.stdout.flush()
            os.system(command)
    
    def process(self, dbname, dbuser, dbhost, backup, backupdir, vacuum, full, reindex, query):
        if (not vacuum) and (not reindex) and (not backup):
            print 'ERROR: Specify at least one operation (backup, vacuum, reindex)!'
            return
        backupdir = os.path.abspath(backupdir)
        if backup:
            self.runCommand('mkdir -p ' + backupdir, query)
        # We will first make the backup of the partitions since they will take 
        # the longest time, while they are being backup it may happen that the LEDDB is updated. 
        # Hence, by doing this we prevent possible data in the diagnostic tables that is not in the referencing tables
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        # Get the cursor
        cursor = connection.cursor()
        for diagTable in (GAIN, QTSTAT, QBSTAT, QFSTAT):
            cursor.execute('select tablename from pg_tables where tablename ~ %s order by tablename desc', [diagTable.lower()+"_part_*",]) #we need to use lower since the table names are always in lower
            partNames = cursor.fetchall()
            for (partName, ) in partNames:
                print '# Processing ' + partName + '...'
                lastBackup = None
                lastestModTime = None
                lastBackupTime = None
                cursor.execute('select max(' + LASTCHECK + ') from ' + partName)
                rows = cursor.fetchall()
                connection.commit() # we do commit to release any lock from the partition
                if len(rows):
                    try:
                        lastestModTime = int(rows[0][0])
                    except:
                        lastestModTime = None
                for btable in os.listdir(backupdir):
                    if btable.count(partName):
                        try:
                            lastBackupTime = int(btable.replace(partName,'').replace('_','').replace('.tgz',''))
                            lastBackup = backupdir + '/' + btable
                        except:
                            lastBackupTime = None
                        break
                
                if (lastestModTime != None) and (lastBackupTime != None) and (lastBackupTime == lastestModTime):
                    # We only can skip the backup of this part if we detect that the already stored backup of this partition has not changed 
                    # (for this we check the last modifaction in the table and compare it with the value stored in the backup name)
                    print '# SKIPPING ' + partName + ' (data has not changed since last backup!)'
                else:
                    if lastestModTime != None:
                        backupFilePath =  backupdir + '/' + partName + '_' + str(lastestModTime) + '.tgz'
                    else:
                        backupFilePath =  backupdir + '/' + partName + '.tgz'
                        
                    if vacuum or full:
                        extrafull = ''
                        if full:
                            extrafull = ' -f '
                        self.runCommand('vacuumdb -d ' + dbname + ' -h ' + dbhost + ' -U ' + dbuser + ' -z -v -t ' + partName + extrafull, query)
                    if reindex:
                        self.runCommand('reindexdb -d ' + dbname + ' -h ' + dbhost + ' -U ' + dbuser + ' -t ' + partName, query)
                    if backup:
                        self.runCommand('pg_dump -i -h ' + dbhost + ' -U ' + dbuser + ' -t ' + partName + ' -v ' + dbname + ' | gzip -c3 > ' + backupFilePath, query)
                        if lastBackup != None:
                            self.runCommand('rm ' + lastBackup, query)
                
                        
        cursor.close()
        connection.close()
        
        # We make a backup of all the LEDDB except the partitions of the DIAG tables
        for table in ALLTABLES:
            if table.count(SEQ) == 0:
                if vacuum or full:
                    extrafull = ''
                    if full:
                        extrafull = ' -f '
                    command = 'vacuumdb -d ' + dbname + ' -h ' + dbhost + ' -U ' + dbuser + ' -v -t ' + table.lower() + extrafull
                    if table not in (GAIN, QTSTAT, QBSTAT, QFSTAT):
                        # For the partitioned tables we do not analyze the parent since the partitions will already be analyzed
                        command += ' -z'
                    self.runCommand(command, query)
                if reindex:
                    self.runCommand('reindexdb -d ' + dbname + ' -h ' + dbhost + ' -U ' + dbuser + ' -t ' + table.lower(), query)
            if backup:
                self.runCommand('pg_dump -i -h ' + dbhost + ' -U ' + dbuser + ' -t ' + table.lower() + ' -v ' + dbname + ' | gzip -c3 > ' + backupdir + '/' + table.lower() + '.tgz', query)
        
