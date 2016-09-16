################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

class GetTestFDTSpeed(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('statuspath', 's', 'Status path', helpmessage = ' where the logs are stored')
        
        # the information
        information = 'Get the speed from the FDT Test'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self, statuspath):
        
        self.statuspath = utils.formatPath(statuspath)
        
        files = sorted(os.listdir(self.statuspath))
        
        count = 0
        sumspeed = 0
        for f in files:
            if f.endswith('log'):
                line = os.popen('tail -n 1 ' + self.statuspath + '/' + f).read().split('\n')[0]
                if line.count('Net Out:') or line.count('Net In:'):
                    fields = line.split()
                    speed = float(fields[7])
                    count += 1
                    sumspeed += speed
                    print  '       ' + str(f) + ' ' + str(fields[1]) + ' ' + str(speed) + ' Mbps'
        
        if sumspeed > 1024.:
            speedstring = '%.3f Gbps' % (sumspeed/1024.)
        else:
            speedstring = '%.3f Mbps' % sumspeed
        print 'TOTAL (' + str(count) + '): ' + speedstring