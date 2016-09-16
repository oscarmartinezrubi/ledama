################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import numpy,os, bisect
from ledama import utils
from ledama.DiagnosticFile import DiagnosticFile
from ledama.leddb.Connector import Connector
from ledama.leddb.Naming import LDS, INITIALUTC, PHASEDIRRA, PHASEDIRDEC,\
    CENTFREQ, BW, NAME, STATION, STATION1, STATION2, TSTEP, FSTEP, VALUES,\
    INITIALMJD, CORR, DIRDEC, DIRRA
from ledama.leddb.query.QueryManager import QueryManager
from ledama.dataanalysis.DiagPlotKey import DiagPlotKey

TIME_NORMAL = 'TIME_UTC_NORMAL'
TIME_WRAP_BLANK = 'TIME_UTC_WRAP_BLANK'
TIME_DAILY = 'TIME_UTC_DAILY'
TIME_HOUR_ANGLE = 'TIME_HOUR_ANGLE'
FREQ_NORMAL = 'FREQ_NORMAL'
WRAP_UTC_TIME_SEP = 200
WRAP_CONF = 60
YAXIS_TYPES = ['amp', 'phase', 'phasewrap', 'real', 'imag']

class DiagnosticLoader:
    """Loader for diagnostic data, preparing it for plotting"""
    def __init__(self, diagnostickey):
        # Set the diagnostic key to know which method to call
        self.diagkey = diagnostickey
        
        # Only used for wrap-time x axis option
        self.tIns = None    
        self.tInsAbs = None
        
        # Other used variables
        self.xaxis = None
       
    def formatStamp(self, x):
        if self.xaxis == TIME_NORMAL:
            return utils.getTimeStamp(x)
        elif self.xaxis == TIME_DAILY:
            return utils.getTimeStamp(x, utils.DAYLY_TIMEFORMAT)
        elif self.xaxis == TIME_HOUR_ANGLE:
            if x >= 0.:
                return utils.getTimeStamp(x, utils.DAYLY_TIMEFORMAT)
            else:
                return '-' + utils.getTimeStamp((-1.*x), utils.DAYLY_TIMEFORMAT)
        elif self.xaxis == TIME_WRAP_BLANK:
            # we need to recover the correct date
            pos = (bisect.bisect(self.tIns, x)) - 1
            if pos >= 0:
                return utils.getTimeStamp((x - self.tIns[pos]) + self.tInsAbs[pos])
            else:
                return utils.getTimeStamp(x)
        else:
            return x

    # Get the initial wrap time for a certain UTC time
    def getInitialWrapTime(self, inUTC, lastT):
        tIn = utils.convertTimeStamp(inUTC)
        if len(self.tInsAbs) == 0:
            # The first record
            t = tIn
            self.tIns.append(tIn)
            self.tInsAbs.append(tIn) 
        elif self.tInsAbs[-1] < tIn:
            # We are in a new time range, we add the absolute time
            self.tInsAbs.append(tIn)
            # This is the separation between LDSs
            t = lastT + WRAP_UTC_TIME_SEP
            
            # We add the relative time (wrapped time)
            self.tIns.append(t)
        elif self.tInsAbs[-1] > tIn + WRAP_CONF:
            raise Exception('Error: not sorted by time')  
        else:
            # We are still in the same time range
            t = self.tIns[-1]        
        return t

    # Load the data using input and save create line plots to be used by plotting device
    def load(self, inputdiag, subplotkey, colorkey, xaxis, resolution, yaxiss, correlations, dbname, dbuser, dbhost, timeout):
        # Get and check the X axis type
        self.xaxis = xaxis
        
        # These are the varibale that we will return
        yss = {} # Dictionary of dictionaries with the several y values. First key is the yaxis, second key is a combined of the subplot index and the color 
        xs = {} # Dictionary of the several x values. The key is a combined of the subplot index and the color (in this case we do not need a key for each yaxis since they have the same x)
        yaxislabels = {}
        subPlotsKeys = None
        
        # Check the given yaxiss and init the yss for each yaxis
        for yaxis in yaxiss:
            if yaxis not in YAXIS_TYPES:
                raise Exception('Y axis must be one or more of ' + ','.join(YAXIS_TYPES))
            yss[yaxis] = {}
            
        # Initialize the connection
        connection = Connector(dbname, dbuser, dbhost).getConnection()

        # Get the unaccepted keys for this diag. type
        errorMessage = 'Error keys: Some of the specified keys contain columns not present in the queried table' 
        
        # Reset the time aux variables
        self.tIns = []
        self.tInsAbs = []
        
        qm = QueryManager()
        queryTable = qm.getQueryTable(self.diagkey)
        names = queryTable.getNames(onlyinforvalues=True)
        qc = None
        if not os.path.isfile(inputdiag):
            # A list of ids
            try:
                ids = utils.getElements(inputdiag)
                if len(ids):
                    qc = qm.initConditions()
                    qm.addCondition(self.diagkey, qc, qm.getQueryTableId(self.diagkey), tuple(ids))
            except:
                raise Exception('Error in inputp argument, wrong path? or error in parsing ids')
        else:
            diagFile = DiagnosticFile(inputdiag)
            if diagFile.queryOption != self.diagkey:
                raise Exception('ERROR: Diagnostic file contains ' + diagFile.queryOption + ' and you are tying to load ' + self.diagkey)
            qc = diagFile.queryConditions
        
        (query, queryDict) = qm.getQuery(self.diagkey, qc, names, [INITIALMJD,CENTFREQ])
        cursor = connection.cursor()
        qm.executeQuery(connection, cursor, query, queryDict, False, timeout)
                    
        # Define the colorBy and subPlotBy diagPlotKeys
        colorBy = DiagPlotKey(colorkey)
        if not colorBy.validate(names):
            raise Exception(errorMessage)
        
        subPlotBy = None
        if subplotkey != None and subplotkey != '':
            subPlotBy = DiagPlotKey(subplotkey)
            if not subPlotBy.compare(colorBy):
                raise Exception('Error in keys: duplicated entry!')
            elif not subPlotBy.validate(names):
                raise Exception(errorMessage)
            subPlotsKeys = {}
        
        if cursor.rowcount == 0:
            connection.close()
            raise Exception('No data found.')
        
        # The dictionaries that contain the several data to be used in both plots (Real-Img or Amp-Phase)
        # The keys will define the different used colors  
        

        xAxisCache = {}
        lastWrapTime = None
        
        # Print the number of loaded records
        print 'Loaded: ' + str(cursor.rowcount) + ' records'
        print 'Data formatting and time conversions...'
        for row in cursor:
            rDict = qm.rowToDict(row, names)
            (lds, inUTC) = (rDict.get(LDS), rDict.get(INITIALUTC)) # LDS related data
            (phaseDirectionRA, phaseDirectionDec) = (rDict.get(PHASEDIRRA),rDict.get(PHASEDIRDEC)) # LDSBP related data
            (sbCentralFreq, sbBW) = (rDict.get(CENTFREQ), rDict.get(BW)) # MS data
            (fStep, values) = (rDict.get(FSTEP),rDict.get(VALUES)) # ALL DIAG
            (stationName,dirRA, dirDec) = (rDict.get(STATION),rDict.get(DIRRA), rDict.get(DIRDEC)) # GAIN related data
            qKind = rDict.get(NAME) # Data related to all Quality diagnostics 
            (stationName1, stationName2) = (rDict.get(STATION1), rDict.get(STATION2)) # QBS related
            tStep = rDict.get(TSTEP) # QTS and GAIN related data
            
            if stationName1 != None and stationName == None:
                stationName = stationName1
            keyDictValues = DiagPlotKey().getAttDict(lds, ('%.3f' % sbCentralFreq), stationName, 0, dirRA, dirDec, stationName2, qKind) 
        
            # Convert values (3D or 4D of floats) into a numpy array (2D or 3D) of complex
            values = utils.convertValues(values)
            # Mask the dummy values
            values[values==-1000-1000j]=numpy.ma.masked
            # Get number of dimensions: 2 (for QFS and QBS) or 3 (for QTS and GAIN)
            dimensions = values.shape
            ndim = len(dimensions)
            res = None
            
            # Get the X-axis depending on the type (we use a cache to avoid generating duplicate the same axis for different rows)
            if xaxis == FREQ_NORMAL:
                cacheKey = (lds, ('%.3f' % sbCentralFreq), fStep, dimensions)
            else:
                cacheKey = (lds, tStep, dimensions)
            
            # Get the xaxis for this row
            if cacheKey in xAxisCache:
                # Read-it from the cache if it was already generated
                (xaxisvalues,res) = xAxisCache[cacheKey]
            else:
                if xaxis != FREQ_NORMAL:
                    if xaxis == TIME_NORMAL:
                        initialTime = utils.convertTimeStamp(inUTC)
                    elif xaxis == TIME_HOUR_ANGLE:
                        initialTime = float(os.popen('taql "meas.hadec([' + str(phaseDirectionRA) + ' deg,' + str(phaseDirectionDec) + ' deg],' + inUTC + ', \'LOFAR\')"').read().split('\n')[1].split(',')[0][1:]) *180./numpy.pi*3600./15.
                    elif xaxis == TIME_DAILY:
                        initialTime = utils.convertTimeStamp(utils.changeTimeFormat(inUTC), timeFormat = utils.DAYLY_TIMEFORMAT)
                    elif xaxis == TIME_WRAP_BLANK:
                        initialTime = self.getInitialWrapTime(inUTC, lastWrapTime)
                    # We regenerate the normal Time Axis
                    if ndim == 2:
                        #It is pol-freq matrix, hence there is not time!
                        numTimeSamples = 1
                    elif ndim == 3:
                        #It is pol-freq-time matrix.
                        numTimeSamples = dimensions[-1]
                    
                    if tStep != None:
                        xaxisvalues = numpy.linspace(initialTime, initialTime+((numTimeSamples-1)*tStep), numTimeSamples)
                    else:
                        xaxisvalues = numpy.array([initialTime, ])
                    # Make modulo in case of hour angle or daily axis 
                    if xaxis == TIME_HOUR_ANGLE:
                        xaxisvalues = numpy.mod(xaxisvalues, 86400)
                        xaxisvalues[xaxisvalues>43200.] -= 86400
                    elif xaxis == TIME_DAILY:
                        xaxisvalues = numpy.mod(xaxisvalues, 86400)
                    lastWrapTime = xaxisvalues[-1]
                    
                    if numTimeSamples >1 and resolution != None and resolution > tStep:
                        res = int(resolution/tStep)
                    
                else:
                    fStepMhz = fStep / 1000000.
                    initialFrequency = sbCentralFreq - (sbBW/2.) + 0.5 * fStepMhz
                    numFreqSamples = dimensions[1]
                    xaxisvalues = numpy.linspace(initialFrequency, initialFrequency+((numFreqSamples-1)*fStepMhz), numFreqSamples)
                    
                    if numFreqSamples > 1 and resolution != None and resolution > fStepMhz:
                        res = int(resolution/fStepMhz)
                        
                # Save the axis in cache
                if res != None:
                    xaxisvalues = xaxisvalues[::res]
                    
                xAxisCache[cacheKey] = (xaxisvalues,res)
                
            #Process the Y axis (average the axis that is not plotted)
            if xaxis == FREQ_NORMAL:
                if ndim == 3:
                    # we need to average the time
                    values = numpy.ma.mean(values, axis=2)
            else:
                # we need to average the frequency
                values = numpy.ma.mean(values, axis=1)
        
            # We itereate over the 4 correlations
            for corIndex in range(len(values)):
                if corIndex in correlations:
                    # FOR EACH ONE OF THE correlation (if it is required)
                    keyDictValues[CORR] = utils.getCorrelationName(corIndex)
                    colorKey = colorBy.getKey(keyDictValues)

                    # We get the related arrays
                    if subPlotBy != None: 
                        subPlotKey = subPlotBy.getKey(keyDictValues)
                        if subPlotKey not in subPlotsKeys:
                            subPlotsKeys[subPlotKey] = []
                        if colorKey not in subPlotsKeys[subPlotKey]:
                            subPlotsKeys[subPlotKey].append(colorKey)
                        key = (subPlotKey, colorKey)
                    else:
                        key = colorKey
                    
                    if key not in xs:
                        xs[key] = numpy.ma.array([],dtype=numpy.float)
                        for yaxis in yaxiss:
                            yss[yaxis][key] = numpy.ma.array([],dtype=numpy.float)
                        
                    if len(values[corIndex].shape) == 1:
                        valuesPol = values[corIndex]
                        if res != None:
                            valuesPol = valuesPol[::res]
                        xs[key] = numpy.append(xs[key], xaxisvalues[~valuesPol.mask])
                        yvalues = valuesPol[~valuesPol.mask]
                        for yaxis in yaxiss:
                            if yaxis == 'amp':
                                valuesappend = numpy.abs(yvalues)
                            elif yaxis == 'phase':
                                valuesappend = numpy.angle(yvalues)
                            elif yaxis == 'phasewrap':
                                valuesappend = numpy.unwrap(numpy.angle(yvalues))
                            elif yaxis == 'real':
                                valuesappend = yvalues.real
                            elif yaxis == 'imag':
                                valuesappend = yvalues.imag
                            yss[yaxis][key] = numpy.append(yss[yaxis][key], valuesappend)
                    else:
                        if ~values.mask[corIndex]:
                            xs[key] = numpy.append(xs[key], xaxisvalues[0])
                            if yaxis == 'amp':
                                valueappend = numpy.abs(values[corIndex])
                            elif yaxis == 'phase' or yaxis == 'phasewrap':
                                valueappend = numpy.angle(values[corIndex])
                            elif yaxis == 'real':
                                valueappend = values[corIndex].real
                            elif yaxis == 'imag':
                                valueappend = values[corIndex].imag
                            yss[yaxis][key] = numpy.append(yss[yaxis][key], valueappend)
        cursor.close()
        
        for yaxis in yaxiss:
            if yaxis == 'amp':
                label = "Amplitude"
            elif yaxis == 'phase':
                label = "Phase[rad]"
            elif yaxis == 'phasewrap':
                label = "Wrap-phase[rad]"
            elif yaxis == 'real':
                label = "Real"
            elif yaxis == 'imag':
                label = "Imaginary"
            yaxislabels[yaxis] = label
        return (xs, yss, yaxislabels, subPlotsKeys)
    
    def close(self):
        self.connection.close()
            
