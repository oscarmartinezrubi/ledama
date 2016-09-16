################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import re,os
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama import utils
from ledama import config as lconfig
from ledama.MSP import MSP as MSPClass
from ledama.leddb.Connector import *
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater
from ledama import tasksdistributor as td

# Add references of data to the LEDDB. This data is in a remote host accessible 
# trough srmls. It will source the GRID_INIT_FILE specified in config 
class AddSRMLs(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('inputpath','i','Path/s',helpmessage='(exclude the part specified in hostpath). There are two options: 1- Provide a ms path (the rest of SBs are considered to be in the same path but changing the SB index). And 2- Provide the parent path (multiple paths are possible. Split them with commas with no white-spaces) and it will look in it/them for the SBs (with a srmls).')
        options.add('hostpath','s','Host path', default='srm://srm.grid.sara.nl:8443',helpmessage=', specify the host:port part of the path (including srm://)')
        options.add('lds','l','LDS name', mandatory=False, helpmessage='to be used. By default the name extracted from the path will be used')
        options.add('versionindex','v','Version Index', mandatory=False, helpmessage=' to be used. By default the version extracted from the path will be used')
        options.add('subbands','b','SubBands', helpmessage=' to search.', default='000-243')
        options.add('store','t','Store', helpmessage=' where data is.',choice=utils.REMOTE_STORES)
        options.add('numprocessors', 'p', 'Simultaneous processes', default = 20)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)   
        options.add('gridinitfile', 's', 'GRID init. file', helpmessage = ', this file is "sourced" in each remote node before voms initialization', default = lconfig.GRID_INIT_FILE)
        # the information
        information = """Add references to LEDDB from a path and a host:port which should be accessible with srmls"""
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Function used for the tasksdistributor   
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        
        (out,err) = td.execute('source ' + self.gridinitfile + '; srmls ' + self.hostpath + absPath)
        if err != '':
            raise Exception(err[:-1])
        connection = Connector(self.dbname, self.dbuser, self.dbhost).getConnection()
        updater = MSPUpdater(connection)      
        diagUpdater = DiagnosticUpdater(connection)
        for m in out.split('\n'):
            if len(m.split()) == 2:
                # Other number of lines are erroneous
                try:
                    msp = MSPClass(absPath, node, (int(m.split()[0]) / 1048576))
                    msp.loadReferencingDataAkin(connection)
                    if self.lds != '':
                        msp.lds = self.lds
                    if self.version != '':
                        msp.versionIndex = int(self.version)
                    updater.process(msp)
                    diagUpdater.updateMeta(msp.msId, True, False, False)
                except Exception,e:   
                    print str(e)
                    print 'Error in:'
                    print  absPath
            else:
                print m
        connection.close()

    def process(self,inputpath,hostpath,lds,versionindex,subbands,store, numprocessors,dbname,dbuser,dbhost,gridinitfile):
        self.gridinitfile = gridinitfile
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.hostpath = hostpath
        self.lds = lds
        self.version = versionindex
        if store not in utils.REMOTE_STORES:
            print 'You must provide a store of  ' + ','.join(utils.REMOTE_STORES)
            return

        # We create the list of paths, we format them
        pathsToCheck = inputpath.split(',')
        
        for i in range(len(pathsToCheck)):
            pathsToCheck[i] = utils.formatPath(pathsToCheck[i])
        
        if len(pathsToCheck) == 1: 
            isSample = pathsToCheck[0].endswith('tar')
        else:
            isSample = False
        
        # We get the bands to use
        bandsToUse = utils.getSubBandsToUse(subbands)
            
        # We get the list of SBs to search depending on each case
        pathsInHost = []
        if isSample:
            if bandsToUse == '':
                print 'If you provide the path of a SB, you must also provide the rest of indexes to look for'
                return
            sbInSample = (re.search('SB[0-9]*', pathsToCheck[0])).group(0)
            for band in bandsToUse:
                pathsInHost.append(pathsToCheck[0].replace(sbInSample, band))
        else:
            for pathToCheck in pathsToCheck:
                resls = os.popen('srmls ' + hostpath + pathToCheck).read().split('\n')
                for element in resls[1:]:
                    if element != '':
                        pathsInHost.append(element.split()[1])
        # We prepare a list of pathsInHost to be checked in current node
        cnodes = []
        for pathInHost in pathsInHost:
            cnodes.append(store)
        if len(pathsInHost) == 0:
            print 'Nothing is found. Are you sure the GRID software is initialized?'
            return
        retValuesKo = td.distribute(cnodes, pathsInHost, self.function, numprocessors, 1, dynamicLog = False)[1]
        td.showKoAll(retValuesKo)