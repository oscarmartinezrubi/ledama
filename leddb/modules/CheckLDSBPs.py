################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.Naming import *
from ledama.leddb.query.QueryManager import QueryManager
from ledama.ReferenceFile import ReferenceFile
from ledama.leddb import LEDDBOps

# CONSTANT
EXPECTED_MAX_SB_INDEX = 243
SIZE_DELTA = 0.95

class CheckLDSBPs(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('ldss','l','LDSs',mandatory=False,helpmessage=', the user can select to check only certain LDS. If multiple provide them comma-separated')
        options.add('opath','o','Output RefFiles path',mandatory=False,helpmessage='where refFiles with the missing MSs of each LDSBP will be written')
        options.add('beama','b','Beam A',default=0)
        options.add('beamb','m','Beam B',default=0)
        options.add('versiona','a','Version A',default=0)
        options.add('versionb','n','Version B',default=0)
        options.add('storea','s','Store A',default=utils.EOR)
        options.add('storeb','t','Store A',default=utils.EORBACKUP)
        options.add('rawa','r','Is A Raw?',default=False)
        options.add('rawb','e','Is B Raw?',default=False)
        options.add('tara','p','Is A Tar?',default=False)
        options.add('tarb','c','Is B Tar?',default=True)
        options.add('bvfa','v','Is A BVF?',default=False)
        options.add('bvfb','f','Is B BVF?',default=False)
        options.add('showko','k','Show only KO messages',default=False)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)   
        # the information
        information = 'for each LDS it compares the MSPs of two LDSBP, LDSBP_A and LDSBP_B.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def getIndexesSizesTuples(self, lds, beam, version, store, raw, tar, bvf):
        indexes = []
        sizes = []
        hosts = []
        absPaths = []
        beamIndexes = []
        
        qm = QueryManager()
        queryOption = MSP_KEY
        names = [SBINDEX, SIZE, HOST, PARENTPATH, NAME, BEAMINDEX]
        qc = qm.initConditions()
        qm.addCondition(queryOption, qc, LDS, lds, '=')          
        qm.addCondition(queryOption, qc, BEAMINDEX, beam, '=')
        qm.addCondition(queryOption, qc, VERSION, version, '=')
        qm.addCondition(queryOption, qc, STORE, store, '=')
        qm.addCondition(queryOption, qc, RAW, raw, '=')
        qm.addCondition(queryOption, qc, TAR, tar, '=')
        qm.addCondition(queryOption, qc, BVF, bvf, '=')
        
        (query, queryDict) = qm.getQuery(queryOption, qc, names, [SBINDEX,])
        
        cursor = self.connection.cursor()
        qm.executeQuery(self.connection, cursor, query, queryDict)
        msps = cursor.fetchall()
        cursor.close()
        
        for msp in msps:
            indexes.append(msp[0])
            sizes.append(msp[1])
            hosts.append(msp[2])
            absPaths.append(msp[3] + '/' + msp[4])
            beamIndexes.append(msp[5])
        return (indexes, sizes, hosts, absPaths, beamIndexes) 
    
    def getErrorString(self, errors):
        message = '  '
        for (index, a, b) in errors:
            message += str(index) + ' ( ' + str(a) + ' : ' + str(b) + ' ), '
        return message[:-2]

    
    def process(self, ldss, opath, beama, beamb, versiona, versionb, storea, storeb, rawa,rawb, tara, tarb,bvfa,bvfb,showko, dbname, dbuser, dbhost):
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS)+ ' can execute this code'
            return
        
        # If output path is specified we create ir if it is not already done
        if opath != '':
            os.system("mkdir -p " + opath)
        
        # Make connection to LEDDB
        self.connection = Connector(dbname, dbuser, dbhost).getConnection()
        
        if ldss != '':
            ldss = ldss.split(',')
        else:
            ldss = LEDDBOps.getTableColValues(self.connection, LDS)
            ldss.sort()
        
        # For each one of the LDS we check the sanity of its MSPs
        for lds in ldss:
            # For this we split the MSPs related to the current LDS in 2 groups (for given versions). 
            # Normal and packed
            # We get the indexes and sizes of the MSPs related such groups
            (aIndexes, aSizes, aHosts, aAbsPaths, aBeamIndexes) = self.getIndexesSizesTuples(lds, beama, versiona, storea, rawa, tara, bvfa)
            (bIndexes, bSizes, bHosts, bAbsPaths, bBeamIndexes) = self.getIndexesSizesTuples(lds, beamb, versionb, storeb, rawb, tarb, bvfb)
            
            if len(aIndexes) == 0 and len(bIndexes) == 0:
                if not showko:
                    print (lds + ' 0 SBs')
            else:
                # We get the maximum index (or 243 if it is lower that 243)
                maxSBIndex = max(aIndexes + bIndexes)
                if maxSBIndex < EXPECTED_MAX_SB_INDEX:
                    maxSBIndex = EXPECTED_MAX_SB_INDEX
        
                missingOK = []
                missingKO = []
                multipleKO = []
                sizeErrors = []
                
                
                misAbsPaths = []
                misNodes = []
                misSizes = []
                misRefFreqs = []
                misBeamIndexes = []
                
                # We try to look subbands within range 0-247 
                for index in range(maxSBIndex+1):
                    
                    num = aIndexes.count(index)
                    numPacked = bIndexes.count(index)
                    
                    if num == 0 and numPacked  == 0:
                        # There are 0 copies of such subband
                        missingOK.append(index)
                    elif num  == 0:
                        # There is no copy in cluster (there is one in b)
                        missingKO.append((index, num, numPacked))  
                    
                        bIndex = bIndexes.index(index)
                        misAbsPaths.append(bAbsPaths[bIndex])
                        misNodes.append(bHosts[bIndex])
                        misSizes.append(bSizes[bIndex])
                        misRefFreqs.append('---')
                        misBeamIndexes.append(bBeamIndexes[bIndex])
                    elif numPacked == 0 :
                        # There is not any b of this SB index
                        missingKO.append((index, num, numPacked))
                        
                        aIndex = aIndexes.index(index)
                        misAbsPaths.append(aAbsPaths[aIndex])
                        misNodes.append(aHosts[aIndex])
                        misSizes.append(aSizes[aIndex])
                        misRefFreqs.append('---')
                        misBeamIndexes.append(aBeamIndexes[aIndex])
                    elif num > 1 or numPacked > 1:
                        # There is some multiple copy
                        multipleKO.append((index, num, numPacked))
                    else:
                        # There is one and one, let's check the size
                        aSize = aSizes[aIndexes.index(index)]
                        bSize = bSizes[bIndexes.index(index)]
                        if (aSize != 0 and bSize == 0) or  (float(aSize) / float(bSize) < SIZE_DELTA):
                            sizeErrors.append((index, aSize, bSize))
                if not showko:
                    print (lds + ' ' + str(maxSBIndex+1) + ' SBs')           
                if len(missingOK) or len(missingKO) or len(multipleKO) or len(sizeErrors):
                    # Some error happened        
                    if not showko and len(missingOK):
                        print ( '   OK Missing: ' + str(len(missingOK)) + ' --> ' + str(missingOK))
                    if len(missingKO):
                        print ( '   KO Missing (a, b): ' + str(len(missingKO)) + ' -->' + self.getErrorString(missingKO))
                        if opath != '':
                            ReferenceFile(opath + '/missing' + lds + '.ref', None, misAbsPaths, misRefFreqs, misSizes, misNodes, misBeamIndexes).write()
                    if not showko and len(multipleKO):
                        print ( '   KO multiple (a, b): ' + str(len(multipleKO)) + ' -->' + self.getErrorString(multipleKO))
                    if not showko and len(sizeErrors):
                        print ( '   Size Errors (a, b): ' + str(len(sizeErrors)) + ' -->' + self.getErrorString(sizeErrors))
        # Close the connection
        self.connection.close()