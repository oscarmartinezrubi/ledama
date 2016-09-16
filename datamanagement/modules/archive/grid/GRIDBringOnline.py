################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import config as lconfig 
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

class GRIDBringOnline(LModule):    
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('hostpath', 's', 'Host path', helpmessage=', specify the host:port part of the path (including srm://)', default='srm://srm.grid.sara.nl:8443')
        options.add('numchecks', 'n', 'Simultaneous GRID checks', default = 244)
        options.add('logspath', 'l', 'Logs path', mandatory = False)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        options.add('gridinitfile', 's', 'GRID init. file', helpmessage = ', this file is "sourced" in each remote node before voms initialization', default = lconfig.GRID_INIT_FILE)
        
        # the information
        information = 'Bring on line files in GRID.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        absPath = what
        command = "source " + self.gridinitfile + "; srm-bring-online " + self.hostpath + absPath
        
        if self.query:
            return (absPath, command)
        else:
            if self.logsPath != None:
                td.execute(command, True) #we redirect output to log
            else:
                (out,err) = td.execute(command)
                if err != '':
                    return (absPath, err[:-1])
                elif out != '':
                    return (absPath, out[:-1])

    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'

    def process(self, reffile, hostpath, numchecks, logspath, query, gridinitfile):
        self.query = query
        self.hostpath = hostpath   
        self.gridinitfile = gridinitfile
        self.logsPath = None
        if (not self.query) and logspath != '':
            self.logsPath = os.path.abspath(logspath)
    
        referenceFile = ReferenceFile(reffile)
        for node in referenceFile.nodes:
            if node not in (utils.SARA, utils.JUELICH):
                print "These script only works with files in " + ','.join((utils.SARA, utils.JUELICH))
                return
                
        # Run it
        # Run it
        retValuesOk = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numchecks, 1, logFolder = self.logsPath)[0]
        for retValueOk in sorted(retValuesOk):
            if retValueOk != None:
                print retValueOk[1]        