################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################


from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.Naming import *
from ledama.leddb import LEDDBOps
import matplotlib
matplotlib.use(lconfig.MATPLOTLIB_BACKEND)
import matplotlib.pyplot as plt 
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator

TIME_FORMAT = "%Y_%m_%d_%H_%M_%S"

class UsedStations(LModule):
    def __init__(self,userName = None):
        self.userName = userName   
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('ldss', 'l', 'LDSs', helpmessage = '. By default all of them are used', mandatory = False)
        options.add('stations', 's', 'Stations', helpmessage =  '. By default all the stations are used', mandatory = False)
        options.add('used', 'u', 'Plot only used stations', default = False)
        options.add('output', 'o', 'Output file', helpmessage = ', if not provided a temporal file is created', mandatory = False)
        options.add('dbname', 'w', 'DB name', default = DEF_DBNAME)
        options.add('dbuser', 'y', 'DB user', default = self.userName)
        options.add('dbhost', 'z', 'DB host', default = DEF_DBHOST)         

        # the information
        information = 'Plots into a file the usage of stations'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def formatStamp(self, x, pos=None):
        return utils.getTimeStamp(x)
    
    # Compute and fill the figure with the required params
    def process(self, ldss, stations, plotUsed, output, dbname, dbuser, dbhost):
        # Make connection to LEDDB
        connection = Connector(dbname, dbuser, dbhost).getConnection()        
        # Initialize the data selection
        dataSelection = {LDS+'.'+NAME: LDSHASSTATION + '.' + LDS}
        
        # Check the given LDSs
        if ldss != '':
            ldss = ldss.split(',')
            if not len(ldss) or ldss[0][0] != 'L':
                print 'No LDSs or incorrect format.'
                return    
            if len(ldss) < LEDDBOps.getTableNumRows(connection, LDS):
                dataSelection[LDSHASSTATION + '.' + LDS] = (tuple(ldss),'IN')
        
        # Check the given stations
        sts = None
        if stations != '':   
            sts = stations.split(',')
            if len(sts) < LEDDBOps.getTableNumRows(connection, STATION):
                dataSelection[LDSHASSTATION + '.' + STATION] = (tuple(sts),'IN')
        else:
            sts = LEDDBOps.getTableColValues(connection, STATION)

        # Perform the query
        rows = LEDDBOps.select(connection, [LDS, LDSHASSTATION], dataSelection, [LDSHASSTATION + '.' + STATION, LDS+'.'+INITIALUTC, LDS+'.'+FINALUTC], toPrint = True)

        if not len(rows):
            print 'No data found in LEDDB with required parameters.'
            return
    
        yStations = {}
        for station in sts:
            yStations[station] = []     
        
        # For each row we add the information to the related station if the times 
        # are the ones required
        for [stationName, inUTC, finUTC] in rows:
            tIn = utils.convertTimeStamp(utils.removeDecimal(inUTC))
            tFin = utils.convertTimeStamp(utils.removeDecimal(finUTC))
            yStations[stationName].append((tIn, (tFin-tIn)))
            
        fig = plt.figure(figsize = (18,10), dpi = 75)
        axes = fig.add_subplot(111)
        axes.hold(False)
        
        yTicks = []
        plotStations = []
        stationCounter = 0
        for station in sts:
            yStation = yStations[station]
            if not plotUsed or (plotUsed and len(yStation)):
                plotStations.append(station)
                axes.broken_barh(yStation, (2 * stationCounter, 2), facecolors='black',)
                yTicks.append(1 + (2*stationCounter))
                stationCounter += 1
        
        axes.set_ylim(0,yTicks[-1]+1)
        axes.xaxis.set_major_locator(MaxNLocator(40))
        axes.xaxis.set_major_formatter(ticker.FuncFormatter(self.formatStamp))
        axes.autoscale(True, axis='both',tight=True)
        axes.set_yticks(yTicks)
        axes.set_yticklabels(plotStations)
        axes.grid(True)
    
        for label in axes.get_xticklabels() + axes.get_yticklabels():
            label.set_fontsize(8)
            
        fig.autofmt_xdate()            
        fig.subplots_adjust(top=0.99)
        fig.subplots_adjust(right=0.99)
        fig.subplots_adjust(left=0.125)
        fig.subplots_adjust(bottom=0.125)
        
        if output == '':
            output = utils.getCurrentTimeStamp(TIME_FORMAT) + 'tempImage.png'
        fig.savefig(output)        