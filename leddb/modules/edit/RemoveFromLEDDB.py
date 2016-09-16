################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
import ledama.config as lconfig
from ledama.leddb.Naming import *
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama import utils
from ledama.leddb import LEDDBOps
    
class RemoveFromLEDDB(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('inputids','i','Ids to delete', mandatory = False, helpmessage=' in the table defined by the type. Take into account that it has cascade effect. For example, deleting LDSs will delete all related LDSBs, LDSBPs, MSs, MSPs and diagnostic tables. For multiple values, use comma-separeted values')
        options.add('inputtype','t','Ids type', mandatory = False, helpmessage=', the top table from which to delete. The tables that in hierarchy below this one will also be affected.', choice=[LDS_KEY, LDSB_KEY, LDSBP_KEY, MS_KEY, MSP_KEY])
        options.add('cleanldsbp','c','Clean LDSBP?', default= False, helpmessage='. Remove the LDSBPs (and all related rows in other tables) which are not referenced in MSP, diagnostic or movies tables and that have a similar LDSBP (same LDS, beam, version and store) which is not empty.')
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)        
        # the information
        information = """Remove data from LEDDB. Two things can be done with this module:
1 - Delete from LEDDB the rows given by the user (via inputids and inputtype)
2 - Clean the empty LDSBP"""
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def deleteLDS(self, connection, ldss, diag = True):
        if type(ldss) == str:
            ldss = (ldss, )
        cursor = connection.cursor()
        if diag:
            cursor.execute('delete from ' + QTSTAT + ' where ' + QTSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ',' + LDSB+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
            cursor.execute('delete from ' + QFSTAT + ' where ' + QFSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ',' + LDSB+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
            cursor.execute('delete from ' + QBSTAT + ' where ' + QBSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ',' + LDSB+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
            cursor.execute('delete from ' + GAIN + ' where ' + GAIN + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ',' + LDSB+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
            cursor.execute('delete from ' + GAINMOVIEHASMS + ' where ' + GAINMOVIEHASMS + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ',' + LDSB+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))         
        cursor.execute('delete from ' + MSP + ' where ' + MSP + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ',' + LDSB+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + MSMETA + ' where ' + MSMETA + '.' + MSMETA+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ',' + LDSB+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + MS + ' where ' + MS+ '.' + LDSBP+ID + ' in (select ' + LDSBP+ '.' + LDSBP+ID + ' from ' + LDSBP+ ',' + LDSB+ ' where ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + LDSBPMETA + ' where ' + LDSBPMETA+ '.' + LDSBPMETA+ID + ' in (select ' + LDSBP+ '.' + LDSBP+ID + ' from ' + LDSBP+ ',' + LDSB+ ' where ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + APPFILE + ' where ' + APPFILE+ '.' + APPRUN+ID + ' IN (select ' + APPRUN+ '.' + APPRUN+ID + ' from ' + APPRUN+ ',' + LDSBP+ ',' + LDSB + ' where ' + APPRUN+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + APPRUN + ' where ' + APPRUN+ '.' + LDSBP+ID + ' in (select ' + LDSBP+ '.' + LDSBP+ID + ' from ' + LDSBP+ ',' + LDSB+ ' where ' + LDSBP+ '.' + LDSB+ID + '=' + LDSB+ '.' + LDSB+ID + ' AND ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + LDSBP + ' where ' + LDSBP+ '.' + LDSB+ID + ' in (select ' + LDSB+ '.' + LDSB+ID + ' from ' + LDSB+ ' where ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + LDSBMETA + ' where ' + LDSBMETA+ '.' + LDSBMETA+ID + ' in (select ' + LDSB+ '.' + LDSB+ID + ' from ' + LDSB+ ' where ' + LDSB+ '.' + LDS + ' IN %s)', (ldss, ))
        cursor.execute('delete from ' + LDSB + ' where ' + LDSB+ '.' + LDS + ' IN %s', (ldss, ))
        cursor.execute('delete from ' + LDSHASSTATION+ ' where ' + LDS + ' IN %s', (ldss, ))
        cursor.execute('delete from ' + LDSMETA + ' where ' + LDSMETA+ID + ' IN %s', (ldss, ))
        cursor.execute('delete from ' + LDS + ' where ' + NAME + ' IN %s', (ldss, ))
        connection.commit()
        cursor.close()
    
    def deleteLDSB(self, connection, ldsbs, diag = True):
        if type(ldsbs) == int:
            ldsbs = (ldsbs, )
        cursor = connection.cursor()
        if diag:
            cursor.execute('delete from ' + QTSTAT + ' where ' + QTSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
            cursor.execute('delete from ' + QFSTAT + ' where ' + QFSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
            cursor.execute('delete from ' + QBSTAT + ' where ' + QBSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
            cursor.execute('delete from ' + GAIN + ' where ' + GAIN + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP+ ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
            cursor.execute('delete from ' + GAINMOVIEHASMS + ' where ' + GAINMOVIEHASMS + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP + ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
        cursor.execute('delete from ' + MSP + ' where ' + MSP + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP + ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
        cursor.execute('delete from ' + MSMETA + ' where ' + MSMETA + '.' + MSMETA+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ',' + LDSBP + ' where ' + MS+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
        cursor.execute('delete from ' + MS + ' where ' + MS+ '.' + LDSBP+ID + ' in (select ' + LDSBP+ '.' + LDSBP+ID + ' from ' + LDSBP + ' where ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
        cursor.execute('delete from ' + LDSBPMETA + ' where ' + LDSBPMETA+ '.' + LDSBPMETA+ID + ' in (select ' + LDSBP+ '.' + LDSBP+ID + ' from ' + LDSBP + ' where ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
        cursor.execute('delete from ' + APPFILE + ' where ' + APPFILE+ '.' + APPRUN+ID + ' IN (select ' + APPRUN+ '.' + APPRUN+ID + ' from ' + APPRUN + ',' + LDSBP+ ' where ' + APPRUN+ '.' + LDSBP+ID + '=' + LDSBP+ '.' + LDSBP+ID + ' AND ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
        cursor.execute('delete from ' + APPRUN + ' where ' + APPRUN+ '.' + LDSBP+ID + ' in (select ' + LDSBP+ '.' + LDSBP+ID + ' from ' + LDSBP + ' where ' + LDSBP+ '.' + LDSB+ID + ' IN %s)', (ldsbs, ))
        cursor.execute('delete from ' + LDSBP + ' where ' + LDSBP+ '.' + LDSB+ID + ' IN %s', (ldsbs, ))
        cursor.execute('delete from ' + LDSBMETA + ' where ' + LDSBMETA+ '.' + LDSBMETA+ID + ' IN %s', (ldsbs, ))
        cursor.execute('delete from ' + LDSB + ' where ' + LDSB+ '.' + LDSB+ID + ' IN %s', (ldsbs, ))
        connection.commit()
        cursor.close()
    
    def deleteLDSBP(self, connection, ldsbps, diag = True):
        if type(ldsbps) == int:
            ldsbps = (ldsbps, )
        cursor = connection.cursor()
        if diag:    
            cursor.execute('delete from ' + QTSTAT + ' where ' + QTSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ' where ' + MS+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
            cursor.execute('delete from ' + QFSTAT + ' where ' + QFSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ' where ' + MS+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
            cursor.execute('delete from ' + QBSTAT + ' where ' + QBSTAT + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ' where ' + MS+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
            cursor.execute('delete from ' + GAIN + ' where ' + GAIN + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ' where ' + MS+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
            cursor.execute('delete from ' + GAINMOVIEHASMS + ' where ' + GAINMOVIEHASMS + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ' where ' + MS+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
        cursor.execute('delete from ' + MSP + ' where ' + MSP + '.' + MS+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ' where ' + MS+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
        cursor.execute('delete from ' + MSMETA + ' where ' + MSMETA + '.' + MSMETA+ID + ' in (select ' + MS+ '.' + MS+ID + ' from ' + MS+ ' where ' + MS+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
        cursor.execute('delete from ' + MS + ' where ' + MS+ '.' + LDSBP+ID + ' IN %s', (ldsbps, ))
        cursor.execute('delete from ' + LDSBPMETA + ' where ' + LDSBPMETA+ '.' + LDSBPMETA+ID + ' IN %s', (ldsbps, ))
        cursor.execute('delete from ' + APPFILE + ' where ' + APPFILE+ '.' + APPRUN+ID + ' IN (select ' + APPRUN+ '.' + APPRUN+ID + ' from ' + APPRUN+ ' where ' + APPRUN+ '.' + LDSBP+ID + ' IN %s)', (ldsbps, ))
        cursor.execute('delete from ' + APPRUN + ' where ' + APPRUN+ '.' + LDSBP+ID + ' IN %s', (ldsbps, ))
        cursor.execute('delete from ' + LDSBP + ' where ' + LDSBP+ '.' + LDSBP+ID + ' IN %s', (ldsbps, ))
        connection.commit()   
        cursor.close()
         
    def deleteMS(self, connection, mss, diag = True):
        if type(mss) == int:
            mss = (mss, )
        cursor = connection.cursor()
        if diag:
            cursor.execute('delete from ' + QTSTAT + ' where ' + QTSTAT + '.' + MS+ID + ' IN %s', (mss, ))
            cursor.execute('delete from ' + QFSTAT + ' where ' + QFSTAT + '.' + MS+ID + ' IN %s', (mss, ))
            cursor.execute('delete from ' + QBSTAT + ' where ' + QBSTAT + '.' + MS+ID + ' IN %s', (mss, ))
            cursor.execute('delete from ' + GAIN + ' where ' + GAIN + '.' + MS+ID + ' IN %s', (mss, ))
            cursor.execute('delete from ' + GAINMOVIEHASMS + ' where ' + GAINMOVIEHASMS + '.' + MS+ID + ' IN %s', (mss, ))
        cursor.execute('delete from ' + MSP + ' where ' + MSP + '.' + MS+ID + ' IN %s', (mss, ))
        cursor.execute('delete from ' + MSMETA + ' where ' + MSMETA + '.' + MSMETA+ID + ' IN %s', (mss, ))
        cursor.execute('delete from ' + MS + ' where ' + MS+ '.' + MS+ID + ' IN %s', (mss, ))
        connection.commit()
        cursor.close()
    
    def deleteMSP(self, connection, msps, diag = True):
        if type(msps) == int:
            msps = (msps, )
        cursor = connection.cursor()
        cursor.execute('delete from ' + MSP + ' where ' + MSP + '.' + MSP+ID + ' IN %s', (msps, ))
        connection.commit()
        cursor.close()
        
    def cleanStore(self, connection, store, duplicated = True):
        #rows = LEDDBOps.select(connection, [LDSBP,LDSB, LDSBPMETA], {LDSBP+'.'+LDSB+ID:LDSB+'.'+LDSB+ID, LDSBPMETA+'.'+LDSBPMETA+ID:LDSBP+'.'+LDSBP+ID, STORE:store,LDSBPNUMMSP:0, LDSBPHASGAIN:False, LDSBPHASQUALITY:False, LDSBPHASGAINMOVIE:False}, columnNames = [LDSBP+ID, LDS, VERSION, BEAMINDEX, ])
        rows = LEDDBOps.select(connection, [LDSBP,LDSB, LDSBPMETA], {LDSBP+'.'+LDSB+ID:LDSB+'.'+LDSB+ID, LDSBPMETA+'.'+LDSBPMETA+ID:LDSBP+'.'+LDSBP+ID, STORE:store,LDSBPNUMMSP:0, LDSBPHASGAIN:False, LDSBPHASQUALITY:False}, columnNames = [LDSBP+ID, LDS, VERSION, BEAMINDEX, ])
        for row in rows:
            (ldsbpId,lds,version,beamIndex) = row
            srows = LEDDBOps.select(connection, [LDSBP,LDSB,LDSBPMETA], {LDSBP+'.'+LDSB+ID:LDSB+'.'+LDSB+ID, LDSBPMETA+'.'+LDSBPMETA+ID:LDSBP+'.'+LDSBP+ID,STORE:store,LDSBPNUMMSP:(0,'!='), LDS: lds, VERSION : version, BEAMINDEX: beamIndex}, columnNames = [LDSBP+ID,])
            if (not duplicated) or len(srows):
                print 'Removing ' + LDSBP + ' row (and related ' + MS + ' rows) ' + '(' + ','.join((str(ldsbpId),lds,str(version),store,str(beamIndex))) + ')'
                self.deleteLDSBP(connection, ldsbpId, diag = False)
    
    def process(self, inputids, inputtype, cleanldsbp, dbname,dbuser,dbhost):
        if not (self.userName in lconfig.FULL_ACCESS_USERS or dbname != DEF_DBNAME):
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        
        if inputids == '' and not cleanldsbp:
            print 'Specify the IDs to remove or activate the cleaning option'
            return
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        
        if cleanldsbp:
            if inputids != '':
                print 'Do not specify inputids and inputtype when activating the cleaning option'
                return
            for (store,) in LEDDBOps.select(connection, [STORE]):
                print 'Cleaning ' + store
                self.cleanStore(connection, store)    
        else:
            if inputtype == LDS_KEY:
                function = self.deleteLDS
            elif inputtype == LDSB_KEY:
                function = self.deleteLDSB
            elif inputtype == LDSBP_KEY:
                function = self.deleteLDSBP
            elif inputtype == MS_KEY:
                function = self.deleteMS
            elif inputtype == MSP_KEY:
                function = self.deleteMSP
            if inputtype == LDS_KEY:
                function(connection, tuple(inputids.split(',')))
            else:
                function(connection, tuple(utils.getElements(inputids)))
        connection.close()
