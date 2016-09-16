################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################


import numpy
import math
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.DiagnosticFile import DiagnosticFile
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.query.QueryManager import QueryManager
import matplotlib
from ledama.leddb.Naming import INITIALUTC, LDS, STATION, TSTEP, VALUES
matplotlib.use(lconfig.MATPLOTLIB_BACKEND)
import matplotlib.pyplot as plt        
import matplotlib.animation as animation

TIME_FORMAT = "%Y_%m_%d_%H_%M_%S"

class GainAnimationSingleStation(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('diagfile', 'i', 'Input DiagnosticFile')
        options.add('correlations', 'g', 'Correlations', helpmessage = ' to plot', default = '0,3')
        options.add('polar', 'p', 'Polar', helpmessage = '. Show Amplitude-Phase instead or Real-Imag', default = False)
        options.add('wrap', 'u', 'Wrap Phase', helpmessage = ' (only apply if polar)', default = False)
        options.add('resolution', 'r', 'Time Resolution', helpmessage = ', it is possible to specify a different time resolution [in seconds] than the step stored in the DB, this will speed up the plots.', mandatory = False)
        options.add('yrange', 'e', 'Y Range', helpmessage = ', specify four values comma separated, i.e. minAmpl,maxAmpl,minPhase,maxPhase in case of polar', mandatory = False)
        options.add('mask', 'm', 'Mask?', helpmessage = ', mask the data when the value is the indicated by maskvalue option', default = False)
        options.add('maskvalue', 'v', 'Mask value', default = '1+0j')
        options.add('output', 'o', 'Output file', helpmessage = ', if not provided a temporal file is created', mandatory = False)
        options.add('timeout', 't', 'Connection timeout', default = 60)
        options.add('dbname', 'w', 'DB name', default = DEF_DBNAME)
        options.add('dbuser', 'y', 'DB user', default = self.userName)
        options.add('dbhost', 'z', 'DB host', default = DEF_DBHOST)        
        # the information
        information = 'Makes an animation of gain of records by time and frequency. Currently only output to file is available.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, )   

    def process(self, diagfile, correlations, polar, wrap, resolution, yrange, mask, maskvalue, output, timeout, dbname, dbuser, dbhost):
        if output != '':
            if not output.lower().endswith('.mp4'):
                output += '.mp4'
        else:
            output = utils.getCurrentTimeStamp(TIME_FORMAT) + 'tempImage.mp4'
        
        # Make connection to LEDDB
        connection = Connector(dbname, dbuser, dbhost).getConnection()       
        cursor = connection.cursor()
        diagFile = DiagnosticFile(diagfile)
        qm = QueryManager()
        names= [LDS, INITIALUTC, STATION, TSTEP, VALUES]
        (query, queryDict) = qm.getQuery(diagFile.queryOption, diagFile.queryConditions, names)
        qm.executeQuery(connection, cursor, query, queryDict, False, timeout)
        
        if cursor.rowcount == 0:
            connection.close()
            print 'No data found.'
            return
        self.mask = mask
        self.wrap = wrap
        self.polar = polar
        self.corrs = utils.getElements(correlations)
        (self.minY0,self.maxY0,self.minY1,self.maxY1) = (None,None,None,None)
        if yrange != '':
            (self.minY0,self.maxY0,self.minY1,self.maxY1) = utils.getFloats(yrange, ',')
        
        xLabel = 'Freq [MHz]'
        if polar:
            y0Label = "Amplitude"
            y1Label = "Phase (rad)"
        else:
            y0Label = "Real"
            y1Label = "Imaginary"
        
        lofardataset = None
        station = None
        self.initialUnixTime = None
        self.tstep = None
        self.data = []
        self.freqAxis = []
        rowIndex = 0 
        
        for row in cursor:    
            rDict = qm.rowToDict(row, names)
            lds  = rDict.get(LDS)
            stationName = rDict.get(STATION)
            tStep = rDict.get(TSTEP)
            values = rDict.get(VALUES)
            rowIndex+=1
        
            if lofardataset == None:
                lofardataset = lds
                station = stationName
                self.initialUnixTime = utils.convertTimeStamp(rDict.get(INITIALUTC))
                self.tstep = tStep
            else:
                if lofardataset != lds or station != stationName or tStep != self.tstep:
                    print 'Data must be related to single LDS and single station and have common time step'
                    return
            
            # values is now a 2D array (pol, time)
            values = numpy.mean(utils.convertValues(values), axis = 1)       
            self.data.append(values)
            self.freqAxis.append(rDict.get('centFreq'))
        cursor.close()
        connection.close()
        
        # Define the resolution of the animation
        self.res = 1
        if resolution != '':
            self.res = int(math.ceil(float(resolution)/self.tstep))
        
        # Now all values is freq,pol,time
        self.data = numpy.array(self.data)
        self.freqAxis = numpy.array(self.freqAxis)
        # Let transpose self.data to have time, pol, freq
        self.data = numpy.ma.array(numpy.transpose(self.data, (2,1,0))[::self.res])        
        if self.mask:
            self.data[self.data==complex(maskvalue)]=numpy.ma.masked
        
        # Initialize the sub-plots
        self.n = 2 # two complex components
        self.m = len(self.corrs) 
        fig, self.axes = plt.subplots(nrows=self.n, ncols=self.m)
        fig.set_size_inches(20,12, forward=True)
        
        self.lines = []
        for compIndex in range(self.n):
            for polIndex in range(self.m):
                if self.m > 1:
                    axexKey = compIndex,polIndex
                else:
                    axexKey = compIndex
                
                # Get the first time sample to initialize the plots
                pdata = self.data[0][self.corrs[polIndex]]
                d = None
                
                if not self.polar:
                    if compIndex == 0:
                        d = pdata.real
                    else:
                        d = pdata.imag
                else:
                    if compIndex == 0:
                        d = numpy.abs(pdata)
                    else:
                        if not self.wrap:
                            d = numpy.angle(pdata)
                        else:
                            d = numpy.unwrap(numpy.angle(pdata))
                            
                self.lines.append(self.axes[axexKey].plot(self.freqAxis, d, animated=False)[0])
                self.axes[axexKey].set_xlim(self.freqAxis[0], self.freqAxis[-1])
                
                if self.minY0 == None:
                    min,max = d.min(), d.max()
                    if min != max:
                        self.axes[axexKey].set_ylim(min, max)
                else:
                    if compIndex == 0:
                        self.axes[axexKey].set_ylim(self.minY0, self.maxY0)
                    else:
                        self.axes[axexKey].set_ylim(self.minY1, self.maxY1)
                    
                if compIndex == 0:
                    self.axes[axexKey].set_title(utils.getCorrelationName(self.corrs[polIndex]))
                    if polIndex == 0:
                        self.axes[axexKey].set_ylabel(y0Label)
                else:
                    self.axes[axexKey].set_xlabel(xLabel)
                    if polIndex == 0:
                        self.axes[axexKey].set_ylabel(y1Label)
                
        plt.subplots_adjust(hspace = .001)
        if self.m > 1:
            axexKey = 0,0
        else:
            axexKey = 0
        self.time_text = self.axes[axexKey].text(0.05, 0.9, '', transform=self.axes[axexKey].transAxes)
        # We'd normally specify a reasonable "interval" here...
        anim = animation.FuncAnimation(fig, self.animate, numpy.arange(0, self.data.shape[0]-1)
                              ,init_func=self.init
                              ,interval=0, blit=True)
        anim.save(output, fps=5)
        
    def animate(self, i):
        toplot = []
        datai = self.data[i+1]
        for j, line in enumerate(self.lines):
            polIndex = j % len(self.corrs)
            if j < len(self.corrs):
                compIndex = 0
            else:
                compIndex = 1
            if self.m > 1:
                axexKey = compIndex,polIndex
            else:
                axexKey = compIndex
            pdata = datai[self.corrs[polIndex]]
            d = None
            if not self.polar:
                if compIndex == 0:
                    d = pdata.real
                else:
                    d =  pdata.imag
            else:
                if compIndex == 0:
                    d = numpy.ma.abs(pdata)
                else:
                    if not self.wrap:
                        d = numpy.ma.array(numpy.angle(pdata), mask=pdata.mask)
                    else:
                        d = numpy.ma.array(numpy.unwrap(numpy.angle(pdata)), mask=pdata.mask)
            if self.mask:
                line.set_ydata(d[~d.mask])
                line.set_xdata(self.freqAxis[~d.mask])
            else:
                line.set_ydata(d)
            if self.minY0 == None:
                ymin,ymax = d.min(), d.max()
                if ymin != ymax:
                    self.axes[axexKey].set_ylim(ymin, ymax)
                    self.axes[axexKey].figure.canvas.draw()
            toplot.append(line)
        self.time_text.set_text(utils.getTimeStamp(self.initialUnixTime + (self.tstep * i * self.res)))
        toplot.append(self.time_text)
        return toplot
    
    def init(self,):
        toplot = []
        for j, line in enumerate(self.lines):
            line.set_ydata(numpy.ma.array(self.freqAxis, mask=True))
            toplot.append(line)
        self.time_text.set_text('')
        toplot.append(self.time_text)
        return toplot        