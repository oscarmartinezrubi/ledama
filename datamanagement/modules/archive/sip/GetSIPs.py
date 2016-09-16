################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import xmlrpclib, os, urllib
from xml.etree.ElementTree import ElementTree
from ledama import utils
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama import tasksdistributor as td
from ledama.datamanagement.sip.SIPIOHandler import SIPIOHandler
from StringIO import StringIO

LTA_USERNAME       = 'AWTIER2EOR'
LTA_PASS       = 'LAqlBMzSvmNT' # in the test is eor123
LTA_XML_RPC_SERVER = 'lofar-ingest.target.rug.nl'
LTA_XML_RPC_PORT   = 9443 # (port of test service, production will be 19443)
    
class GetSIPs(LModule):    
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('inputdp','d','Input DataProduct',mandatory=False,helpmessage=' table file (or single DataProduct Id), use https://lofar.astron.nl/mom3/. Find your observation and beam. Click in Details and DataProducts and copy/paste the table in a file.')
        options.add('inputobsid','i','Observation Id',mandatory=False,helpmessage=', use the LTA catalog (http://lofar.target.rug.nl/) to find the Observation Id')
        options.add('project', 'p', 'Project')
        options.add('output','o','Output folder for the SIPs')
        options.add('numconnections','n','Number of simultaneous connections to ' + LTA_XML_RPC_SERVER, default=5)
        # the information
        information = """Get the SIPs from one of 2 options (in both options you have to manually specify the project): 
1 - From copy-paste table download from MOM (or a single DataProduct Id)
2 - From an observation Id from the LTA catalog"""
        # Initialize the parent class
        LModule.__init__(self, options, information)

    # Function used for the tasksdistributor to change a SB
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (mom_id,project) = what
        server   = xmlrpclib.ServerProxy('https://%s:%s@%s:%d' % (LTA_USERNAME, LTA_PASS, LTA_XML_RPC_SERVER, LTA_XML_RPC_PORT))
        sipioh = SIPIOHandler()
        
        rpc_dict = server.GetSip(project, long(mom_id))
        
        if rpc_dict['result'] != 'ok':
            raise Exception('ERROR in getting sip for ' + mom_id)
        else:
            tempsippath = self.output + '/tempsip' + mom_id
            sipFile = open(tempsippath, 'wb')
            sipFile.write(rpc_dict['sip'])
            sipFile.close()
            
            sipioh.read(tempsippath)
            msName = sipioh.sip.get_dataProduct().get_fileName()
            sipPath = sipioh.getSIPPath(self.output+'/'+msName)
            os.system('mv ' + tempsippath + ' ' + sipPath)

    def process(self, inputdp, inputobsid, project, output, numconnections):  
        if (inputdp == '' and inputobsid == '') or (inputdp != '' and inputobsid != ''):
            print 'ERROR: specify one of (inputdp, inputobsid)'
            return
        # Create output directoty
        self.output = utils.formatPath(output)
        os.system('mkdir -p ' + self.output )
        childs = []
        whats = []
        if inputdp != '':
            if os.path.isfile(inputdp):
                for line in open(inputdp,'r').read().split('\n'):
                    fields = line.split()
                    if len(fields) == 12:
                        childs.append('CHILD')
                        whats.append((str(fields[2]),project))    
            else:
                childs.append('CHILD')
                whats.append((inputdp,project))
        else: #inputobsid != ''
            # login; this will create a session which is valid for 30 days
            # the session_id can be reused or requested every time
            username = 'AWTIER2EOR'
            password = 'LAqlBMzSvmNT'
            login_url = 'https://lofar-login.target.rug.nl/Session?request_session=default&login_server_key=astrowize&username=%s&password=%s' % (username, password)
            xml = urllib.urlopen(login_url).read()
            session_id = ElementTree().parse(StringIO(xml)).text
            
            # get the DataProduct's for the given observation ID
            # session_id must be defined from above call to login service
            server_port = 'lofar.target.rug.nl'
            url = 'http://%s/Lofar?mode=urls&observation_id=%s&project=%s&_session=%s' % (server_port, str(inputobsid), project, urllib.quote(session_id))
            dataproducts = urllib.urlopen(url).read()
            for dataproduct_id, url, filesize, md5_hash in [line.split('|') for line in dataproducts.splitlines()] :
                childs.append('CHILD')
                whats.append((str(dataproduct_id),project))  
        (retValuesOk, retValuesKo) = td.distribute(childs, whats, self.function, numconnections, 1)
        print 'Num. Generated SIPs: ' + str(len(retValuesOk))
        if len(retValuesKo):
            print 'Num. Errors: ' + str(len(retValuesKo))
            print 'Example: '
            print
            print retValuesKo[0]