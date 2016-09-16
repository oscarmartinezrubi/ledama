################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, time
import ledama.tasksdistributor as td
from ledama import utils
from ledama.ReferenceFile import ReferenceFile
from ledama import msoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
import ledama.config as lconfig

# String Constants (TXs from SARA only work in node078 or node080)
NODES_GRID = ['node078','node080']

#KEYWORDS for nodes
NO_NODES_KEYWORD = 'NO_KEYWORD'
SAME_NODES_KEYWORD = 'USE_SAME_NODES_AS_INPUT_REFFILE'
OPTBACKUP_NODES_KEYWORD = 'OPTBACKUP_EACH_MSP_COPIED_IN_NEXT_NODE'
NODES_KEYWORDS = [NO_NODES_KEYWORD, SAME_NODES_KEYWORD, OPTBACKUP_NODES_KEYWORD]

MAX_JUELICH = 5

class CopyData(LModule):    
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('output', 'o', 'Output RefFile', helpmessage = ' describing where the data will be copied', mandatory = False)
        options.add('copypath', 'c', 'Copy path', helpmessage = ' where data will be copied in the selected nodes. In the copy path it is possible to use XXXXX or XXXX_XXXXX, it will be replaced for the proper characters in each case if the information can be retrieved from the paths of the original data (In this case, also pipeline and versions will be automatically added to the path if required).', default = '/data1/users/lofareor/pipeline/LXXXX_XXXXX')
        options.add('nodeskey', 'k', 'Destination nodes key', default = NO_NODES_KEYWORD, choice = NODES_KEYWORDS)
        options.add('nodes', 'u', 'Destination nodes', helpmessage = ' where the data will be copied (only used if ' + NO_NODES_KEYWORD + ' is selected)', mandatory = False)
        options.add('subbands', 'b', 'SubBands to copy', helpmessage = '. The user can specify to copy only some SBs of the RefFile. For example if we want to copy only SBs 0,1,2,4 and 8 we should use 0-4,8. By default all the SBs are copied.', mandatory = False)
        options.add('resume', 'r', 'Resume copy', helpmessage = '. For each SB, if in the destination path there is already a file/folder with same name and size, the copy command is not executed', default = False)
        options.add('numprocessors', 'p', 'Simultaneous processes per node', helpmessage = ' receiving data', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them (if specified, it will also create the output reference file)', default = False)
        if userName in lconfig.FULL_ACCESS_USERS:
            options.add('statuspath', 's', 'Status path', helpmessage = ' to use for the transfer status (logs and times)', default = 'logs', mandatory = False)
        else:
            options.modifyOption('copypath', default = '/data1/users/lofareor/' + userName + '/LXXXX_XXXXX')
            options.modifyOption('numnodes', default = 1)
        # the information
        information = 'Copy the data in a RefFile to the location (path and node) specified by the user.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)
    
    def inEoR(self, node):
        return (node.count('node') != 0)
    def inCEP1(self, node):
        return (node.count('lce') > 0) or (node.count('lse') > 0)
    def inCEP2(self, node):
        return (node.count('locus') != 0)
    
    def getCommands(self, sbPath, node, sbPathCopy, nodeCopy, currentNode):
        # The commands to be executed
        commandSize = None # Size check
        commandMkdir = None # mkdir in remote host
        commandCopy = None # the copy command
        
        if not self.inEoR(nodeCopy) and nodeCopy not in (utils.TARGET_E_EOR, utils.TARGET_F_EOR, utils.TARGET_E_OPS, utils.SOUTHAMPTON):
            raise Exception('No function defined for moving to ' + nodeCopy)
        
        # If it is not a file, we need to add recursive in the copy methods
        recursiveArg = ''
        if not sbPathCopy.endswith('tar'):
            recursiveArg = '-r'
        parentPath = os.path.abspath(os.path.join(sbPathCopy, '..'))
        
        # First we define the commands for size checking and mkdir
        if self.inEoR(nodeCopy): # We want to copy to the EoR
            if self.inEoR(currentNode): # We are in the EoR
                commandSize = "ssh " + nodeCopy + " '" + "du -sm " + sbPathCopy + "'"
                commandMkdir = "ssh " + nodeCopy + " '" + "mkdir -p " + parentPath + "'"
            else: # We are not in EoR 
                commandSize = "ssh " + self.userName + "@lofareor01.target.rug.nl 'du -sm " + "/net/" + nodeCopy + sbPathCopy + "'"
                commandMkdir = "ssh " + self.userName + "@lofareor01.target.rug.nl 'mkdir -p " + "/net/" + nodeCopy + parentPath + "'"
        elif nodeCopy == utils.SOUTHAMPTON:
                commandSize = "ssh as25g11@lofar1.phys.soton.ac.uk '" + "du -sm " + sbPathCopy + "'"
                commandMkdir = "ssh as25g11@lofar1.phys.soton.ac.uk '" + "mkdir -p " + parentPath + "'"
        
        # Now we define the copy command
        if self.inEoR(currentNode): # we are in EoR 
            if self.inEoR(nodeCopy): # we want to copy to EoR
                if node == nodeCopy: # the data is in the same node
                    commandCopy = "ssh " + nodeCopy + " 'cd " + parentPath + " ; cp " + recursiveArg + " " + sbPath + " ." + "'"
                elif self.inEoR(node): # the data is in EoR
                    commandCopy = "ssh " + nodeCopy + " 'cd " + parentPath + " ; scp " + recursiveArg + " " + node + ':' + sbPath + " ." + "'"
                elif node in (utils.SARA, utils.JUELICH, utils.TARGET_E_OPS, utils.TARGET_E_EOR, utils.TARGET_F_EOR): # Data is out of the EOR, should be LTA
                    if node == utils.SARA:
                        commandCopy = 'source ' + lconfig.GRID_INIT_FILE + '; srmcp -debug=true  -streams_num=35 -server_mode=passive -buffer_size=2097152 -tcp_buffer_size=2097152 -retry_num=0 -globus_tcp_port_range=20000,25000 srm://srm.grid.sara.nl:8443' + sbPath + ' file:///' + '/net/' + nodeCopy + sbPathCopy
                        #command2 = 'srmcp -debug=true -streams_num=10 -server_mode=passive -buffer_size=1000000 -tcp_buffer_size=1000000 -retry_num=0 -globus_tcp_port_range=20000,25000 srm://srm.grid.sara.nl:8443' + sbPathInCurrentNode + ' file:///' + '/net/' + nodeToCopyData + sbPathToCopyDataTo
                    elif node == utils.JUELICH:
                        #command2 = 'ssh ' + nodeToCopyData + ' "wget --http-user=martinez --http-password=dsLOFARtestOM --content-disposition http://dcachepool3.fz-juelich.de/webserver-lofar/SRMFifoGet.py?surl=srm://lofar-srm.fz-juelich.de:8443' + sbPathInCurrentNode + ' -O ' + sbPathToCopyDataTo + '"' 
                        commandCopy = 'ssh ' + nodeCopy + ' "wget --http-user=martinez --http-password=Lofardata2012 --no-check-certificate --content-disposition https://lofar-download.fz-juelich.de/webserver-lofar/SRMFifoGet.py?surl=srm://lofar-srm.fz-juelich.de:8443' + sbPath + ' -O ' + sbPathCopy + '"'
                    elif node == utils.TARGET_F_EOR:
                        commandCopy = 'ssh ' + nodeCopy + " 'java -jar " + lconfig.FDT_PATH + " -p 20002 -bio -noupdates -silent -c lotar1.staging.lofar -pull -r -d " + parentPath + " " + sbPath + "'"
                    else: #node == utils.TARGET_E_OPS or node == utils.TARGET_E_EOR:
                        #command2 = 'ssh ' + nodeToCopyData + ' "source ' + lconfig.GRID_INIT_FILE + ' ; globus-url-copy gsiftp://lotar1.staging.lofar/' + sbPathInCurrentNode + ' file:///' + sbPathToCopyDataTo + '"'
                        ##command2 = 'ssh ' + nodeToCopyData + ' srmcp -debug -streams_num=1 -send_cksm=false gsiftp://lotar1.staging.lofar/' + sbPathInCurrentNode + ' file:///' + sbPathToCopyDataTo
                        commandCopy = 'ssh ' + nodeCopy + " 'java -jar " + lconfig.FDT_PATH + " -p 20001 -bio -noupdates -silent -c lotar1.staging.lofar -pull -r -d " + parentPath + " " + sbPath + "'"
            elif nodeCopy in (utils.TARGET_E_OPS, utils.TARGET_E_EOR):
                commandCopy = 'ssh ' + node + " 'java -jar " + lconfig.FDT_PATH + " -c lotar1.staging.lofar -p 20001 -noupdates -silent -r -d " + msoperations.getParentPath(sbPathCopy) + " " + sbPath + "'"
            elif nodeCopy == utils.TARGET_F_EOR:
                commandCopy = 'ssh ' + node + " 'java -jar " + lconfig.FDT_PATH + " -c lotar1.staging.lofar -p 20002 -noupdates -silent -r -d " + msoperations.getParentPath(sbPathCopy) + " " + sbPath + "'"
            elif nodeCopy == utils.SOUTHAMPTON:
                commandCopy = 'ssh ' + node + " 'scp " + recursiveArg + ' -c blowfish -C ' + sbPath  + " as25g11@lofar1.phys.soton.ac.uk:" + parentPath + "'"
        elif self.inEoR(nodeCopy): # we are not in EoR but we want to copy to EoR
            if self.inCEP1(currentNode) and self.inCEP1(node): # We are in CEP1 and we want to copy from CEP1 to EoR 
                commandCopy = 'scp ' + recursiveArg + ' -c blowfish -C ' +  node + ':' + sbPath  + " " + self.userName + "@lofareor01.target.rug.nl:" + "/net/" + nodeCopy + parentPath
            elif self.inCEP2(currentNode) and self.inCEP2(node): # We are in CEP2 and we want to copy from CEP2 to EoR
                commandCopy = 'ssh ' + node + " 'java -jar " + lconfig.FDT_PATH + " -p 40 " + nodeCopy.replace('node','') + " -c lofareor02.target.rug.nl " + sbPath + " -d " + parentPath + " -r'" 
        else: # We are not in EoR and we do not want to copy to EoR (we assume nodes are visible)
            commandCopy = 'scp ' + recursiveArg + ' -c blowfish -C ' +  node + ':' + sbPath  + " " + nodeCopy + ":" + parentPath
            
        if commandCopy == None:
            raise Exception('No function defined for moving from ' + node + ' to ' + nodeCopy)
        
        return (commandSize, commandMkdir, commandCopy)
             
    # Function used for the tasksdistributor to copy a SB
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (sbPath, node, sbPathCopy, size, nodeExtra) = what
        if nodeExtra != None:
            nodeCopy = nodeExtra
        else:
            nodeCopy = identifier
        currentNode = utils.getHostName()
        # Get the commands to be executed (depends on where we are, where data is and where data will be)
        (commandSize, commandMkdir, commandCopy) = self.getCommands(sbPath, node, sbPathCopy, nodeCopy, currentNode)
        sbName = msoperations.getMeasurementSetName(sbPath)
        # Execute the commands
        if commandCopy != None:
            tx = True
            if self.resume and not self.query:
                if commandSize == None:
                    raise Exception('Error: Can not resume copies for node=' + node + ', nodeCopy=' + nodeCopy + ', currentNode=' + currentNode)
                else:
                    try:
                        out = td.execute(commandSize)[0]
                        observedsize = int((out.split('\t'))[0])
                        if not (observedsize < 0.99 * size):
                            tx = False
                    except ValueError:
                        pass
            if tx:
                if self.query:
                    if commandMkdir != None:
                        return (sbName, commandMkdir + '\n' + commandCopy)
                    else:
                        return (sbName, commandCopy)
                else:
                    tstart = time.time()
                    if commandMkdir != None:
                        td.execute(commandMkdir, True) #we redirect output to log
                    td.execute(commandCopy, True) #we redirect output to log
                    if self.statuspath != '':
                        td.execute("echo " + str(time.time()-tstart) + ' > ' + self.statuspath + '/' + sbName + ".time")
            else:
                return (sbName, 'No copy required for ' + nodeCopy + ' ' + sbPathCopy)
        else:
            raise Exception('Error generating copy commands for node=' + node + ', nodeCopy=' + nodeCopy + ', currentNode=' + currentNode)
        
    def getTD(self,referenceFile,nodeskey,nodes,subbands,copypath,output):
        try:
            # Get the indexes of the MSP to be copied depending on the SB indexes
            indexes = []
            if subbands == '':
                # We get all the indexes
                indexes = range(len(referenceFile.absPaths))
            else:
                bandsToUse = utils.getElements(subbands)
                # We only get the indexes indicated by subbands
                for i in range(len(referenceFile.absPaths)):
                    if msoperations.getSBIndex(referenceFile.absPaths[i]) in bandsToUse:
                        indexes.append(i)
            if len(indexes) == 0:
                print 'No data pointed for the selected bands'
                return (None,None)
    
            # We get the nodes to use and the max MSP per node if no keyword is specified
            if nodeskey == NO_NODES_KEYWORD:
                if nodes == '':
                    print 'None nodes selected'
                    return (None,None)    
                nds = utils.getNodes(nodes)
                indexessubarrays = utils.splitArray(indexes, len(nds))
                nodesToUse = {}
                for i in range(len(nds)):
                    for j in range(len(indexessubarrays[i])):
                        nodesToUse[indexessubarrays[i][j]] = nds[i]
            # variables to write the output "input reffile"
            iabsPaths=[]
            inodes=[]
            # variables to write the output "output reffile"
            mabsPaths=[]
            mrefFreqs=[]
            msizes=[]
            mnodes=[]
            mbeamIndexes=[]
            
            whats = []
            identifiers = []
        
            for i in indexes:
                msAbsPath = referenceFile.absPaths[i]
                msName = msoperations.getMeasurementSetName(msAbsPath)
                msNode = referenceFile.nodes[i]
                msSize = referenceFile.sizes[i]
                if copypath.count('XXXXX'): # If XXXXX is specified we need to make some replacements in the destination path of this SB
                    ldsNumber = msoperations.getLDSName(msAbsPath)[1:]
                    if copypath.count('XXXX_XXXXX'): # Also the year need to be replaced
                        year = msoperations.getYear(msAbsPath)
                        if year == None:
                            print 'Error replacing XXXX: Can not get the year from the initial path in ' + msAbsPath
                            return (None, None)
                        mpath = copypath.replace('XXXXX', ldsNumber).replace('XXXX', year)
                    else:
                        mpath = copypath.replace('XXXXX', ldsNumber)
                    if (not msoperations.isRaw(msName)) and (mpath.count('pipeline') == 0) and (mpath.count('lofareor') != 0):
                        # add pipeline sub-folder
                        mpath = mpath.replace('lofareor','lofareor/pipeline')      
                    # If the user did not specify a version we add the same than the input one
                    version = msoperations.getVersionIndex(msAbsPath)
                    versionnew = msoperations.getVersionIndex(mpath)
                    if versionnew == 0 and version != versionnew:
                        mpath +=  '_%03d' % version
                else:
                    mpath = copypath
                # Full moving path is formed   
                mpath +=  '/' + msName
                
                if nodeskey == SAME_NODES_KEYWORD:
                    mnode = (msNode)
                elif nodeskey == OPTBACKUP_NODES_KEYWORD:
                    mnode = (utils.getNextNode(msNode))
                else:
                    mnode = nodesToUse[i]
                # Add this ms for the output "input reffile"
                iabsPaths.append(msAbsPath)
                inodes.append(msNode)
                # Add this ms for the output "output reffile"
                mabsPaths.append(mpath)
                mnodes.append(mnode)
                msizes.append(msSize)
                mrefFreqs.append(referenceFile.refFreqs[i])
                mbeamIndexes.append(referenceFile.beamIndexes[i])
                                
                if mnode in (utils.TARGET_E_OPS, utils.TARGET_E_EOR, utils.TARGET_F_EOR, utils.SOUTHAMPTON):
                    # If we are moving out of the EoR cluster (Target or Southampton)
                    # we use the node (as identifier, i.e. for distribution purposes) 
                    # where the original data is
                    whats.append((msAbsPath, msNode, mpath, msSize, mnode))
                    identifiers.append(msNode)
                else:
                    # If we are moving to the EoR cluster
                    # we use the node (as identifier) where the copy data will be
                    whats.append((msAbsPath, msNode, mpath, msSize, None))
                    identifiers.append(mnode)
                
            if output != '':
                # Create the output RefFile if required                
                ReferenceFile(output, None, mabsPaths, mrefFreqs, msizes, mnodes, mbeamIndexes).write()
                if not self.query and self.statuspath != '':
                    os.system("cp " + output + ' ' + self.statuspath + '/OUT_' + os.path.basename(output))
                    oInFile = self.statuspath + '/IN_' + os.path.basename(output)
                    if os.path.isfile(oInFile):
                        os.system('rm ' + oInFile)
                    ReferenceFile(oInFile, None, iabsPaths, mrefFreqs, msizes, inodes, mbeamIndexes).write()
            return (identifiers,whats)
        except Exception, e:
            print str(e)
            return (None, None)
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what[0]) + '.log'    
    
    def process(self, reffile, output, copypath, nodeskey, nodes, subbands, resume, numprocessors, numnodes, query, statuspath = ''):
        self.query = query        
        self.resume = resume
        self.statuspath = utils.formatPath(statuspath)
        logsPath = None
        if self.statuspath != '':
            self.statuspath = os.path.abspath(self.statuspath)
            os.system('mkdir -p ' + self.statuspath)
            if output == '':
                print 'If you specify statuspath you must also specify output'
                return
            if (not self.query):
                logsPath = self.statuspath
        # Check the input reffile
        referenceFile = ReferenceFile(reffile)
        if not len(referenceFile.absPaths):
            print 'No data in this reference file'
            return
        # Check for already existing output files
        if output != '' and os.path.isfile(output):
            print 'Error: ' + output + ' already exists'
            return

        if utils.SARA in referenceFile.nodes:
            if utils.getHostName() not in NODES_GRID:
                print 'Error: copies from ' + utils.SARA + ' requires to run in ' + str(NODES_GRID)
                return
        elif utils.JUELICH in referenceFile.nodes:
            if numprocessors * numnodes > MAX_JUELICH:
                print 'Error: In copies from ' + utils.JUELICH + ' the number of simult. processes is ' + str(MAX_JUELICH)
                return

        (identifiers,whats) = self.getTD(referenceFile,nodeskey,nodes,subbands,utils.formatPath(copypath),output)
        if identifiers != None and whats != None:
            (retValuesOk, retValuesKo) = td.distribute(identifiers, whats, self.function, 
                numprocessors, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
            td.showOk(retValuesOk)
            td.showKoFirst(retValuesKo)