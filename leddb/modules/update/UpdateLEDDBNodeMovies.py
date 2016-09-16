################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import time, os
from ledama import utils
from ledama import config as lconfig
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.MovieInfoFile import MovieInfoFile
from ledama.leddb import LEDDBOps
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *

INFO = 'INFO'

class UpdateLEDDBNodeMovies(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('mpath','i','Path',default='/data3/users/lofareor/movies', helpmessage=' where to find the movies.')
        options.add('numprocessors','p','Simultaneous processors',default=1)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)        
        # the information
        information = 'update the LEDDB movies with the contained movies in current node'        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        movieFolder = what
        connection = Connector(self.dbName, self.dbUser , self.dbHost).getConnection()
        content = os.listdir(movieFolder)
        isAdded = False
        isUpdated = False
        if INFO not in content:
            raise Exception(INFO + ' file is not found in ' + movieFolder)
        # Read the MovieInfoFile
        mifile = MovieInfoFile(movieFolder + '/' + INFO)
        try:
            mifile.read()
        except:
            errormessage = 'Error adding GAIN movie in ' + movieFolder + ': Movie without used data'
            raise Exception(errormessage)
        # Get the movie path
        moviePath = None
        for element in content:
            if element.lower().endswith('mp4'):
                moviePath = movieFolder + '/' + element
        if moviePath == None:
            raise Exception('None movie (mp4) found in ' + movieFolder)
        
        lastModification = utils.getCurrentTime(utils.getLastModification(movieFolder))
        rows = LEDDBOps.select(connection, GAINMOVIE, {(FILEPATH):moviePath,HOST:self.hostName}, columnNames = [GAINMOVIE+ID,LASTCHECK])
        if len(rows):
            (gainMovieId,lastCheck) = rows[0]
            if lastCheck > lastModification:
                connection.close()
                # The last modification happened before the last checking
                # which means there is nothing to update
                print 'No changes in ' + movieFolder
                return (isAdded, isUpdated)
            else:
                print 'Updating ' + movieFolder
                isUpdated = True
                # The movie has probably changed. Let's remove the old reference
                self.deleteLEDDBMovie(connection, gainMovieId)                  
        else:
            print 'Adding ' + movieFolder
            isAdded = True
            
        msIds = []
        for msData in mifile.useddata:
            if len(msData) == 3:
                msIds.append(msData[0])
            elif len(msData) == 2:
                (node,mspath) = msData
                rows = LEDDBOps.select(connection, MSP, {NAME:msoperations.getMeasurementSetName(mspath), PARENTPATH:msoperations.getParentPath(mspath), HOST:node}, columnNames = [MS+ID])
                if len(rows) == 1:
                    msIds.append(rows[0][0])
                else:
                    connection.close()
                    errormessage = 'Error adding GAIN movie in ' + movieFolder + ': None MS found in ' + node + ':' + mspath
                    if isUpdated:
                        errormessage += '. Old reference was deleted but could not add new movie'
                    raise Exception(errormessage)

        selection = {FILEPATH : moviePath, 
                     HOST : self.hostName, 
                     SIZE : msoperations.getSize(moviePath), 
                     XAXIS : mifile.xaxis, 
                     JONES : mifile.jones, 
                     POLAR : mifile.polar, 
                     TIMES : mifile.times, 
                     CHANNELS : mifile.channels, 
                     YRANGE : str(mifile.yrange), 
                     LASTCHECK : self.currentTime}
        updating = {REFSTATION : mifile.refstation, 
                    MESSAGE : mifile.message}
        # Add the gain movie in the GAINMOVIE table and get the ID        
        gainMovieId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(connection, GAINMOVIE, selection, updating, [GAINMOVIE+ID,]))
        # Add the references to the GAINMOVIEHASSTATION
        for stIndex in range(len(mifile.stations)):
            LEDDBOps.updateUniqueRow(connection, GAINMOVIEHASSTATION, {GAINMOVIE+ID : gainMovieId, STATION : mifile.stations[stIndex], DELAY : mifile.delays[mifile.stations[stIndex]]},)
        # Add the references to the GAINMOVIEHASMS and update the meta-data
        diagUpdater = DiagnosticUpdater(connection)
        for msId in msIds:
            LEDDBOps.updateUniqueRow(connection, GAINMOVIEHASMS, {GAINMOVIE+ID : gainMovieId, MS+ID: msId},)
            diagUpdater.updateMeta(msId, hasGainMovie = True)
        connection.close()
        
        return (isAdded, isUpdated)
    
    def getMoviesFolders(self, parentPath):
        moviesFolders = []
        for dirpp in os.listdir(parentPath):
            for subdir in os.listdir(parentPath + '/' + dirpp):
                moviesFolders.append(parentPath + '/' + dirpp + '/' + subdir)
        return moviesFolders
    
    def deleteLEDDBMovie(self, connection, gainMovieId):
        LEDDBOps.delete(connection, GAINMOVIEHASSTATION, {GAINMOVIE+ID:gainMovieId,})
        LEDDBOps.delete(connection, GAINMOVIEHASMS, {GAINMOVIE+ID:gainMovieId,})
        LEDDBOps.delete(connection, GAINMOVIE, {GAINMOVIE+ID:gainMovieId,})    
    
    def process(self, mpath, numprocessors, dbname, dbuser, dbhost):
        self.dbName = dbname
        self.dbUser = dbuser
        self.dbHost = dbhost
        self.hostName = utils.getHostName()
        
        # Get current time stamp, all the updated measurement sets will have a 
        # higher time stamp
        self.currentTime = utils.getCurrentTime() 
        tstart = time.time()
        print utils.getCurrentTimeStamp() + ' - starting LEDDB movies updater in ' + self.hostName + '...'
         
        moviesFolders = self.getMoviesFolders(os.path.abspath(mpath)) 
        numMovies = len(moviesFolders)
        if not numMovies:
            print 'No movies found in ' + self.hostName + ':' + mpath
            return
        # Use tasks distributor to add each movie to the LEDDB
        parents = []
        for i in range(numMovies): parents.append('parent')
        (retValuesOk, retValuesKo) = td.distribute(parents, moviesFolders, self.function, numprocessors, 1, dynamicLog = False)
        # Collect the result from the distributor
        processedKo = len(retValuesKo)
        td.showKoAll(retValuesKo)
        addedCounter = 0
        updatedCounter = 0
        for (isAdded, isUpdated) in retValuesOk:
            if isAdded:
                addedCounter += 1
            if isUpdated:
                updatedCounter += 1
                
        print 'Cleaning old references'
        removedCounter = 0
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        for (gainMovieId,gainMoviePath) in LEDDBOps.select(connection, GAINMOVIE, {(LASTCHECK):(self.currentTime,'<'),}, columnNames = [GAINMOVIE+ID,FILEPATH]):
            if not os.path.isfile(gainMoviePath):
                # It does not exist anymore, we can delete its related entries from the DB
                removedCounter += 1
                print 'Removing movie ' + gainMoviePath
                self.deleteLEDDBMovie(connection, gainMovieId)
                
        # We set to False all the rows in MSMETA where the MSHASGAINMOVIE is None (NULL)
        LEDDBOps.update(connection, MSMETA, {MSHASGAINMOVIE:False}, {MSHASGAINMOVIE:None})
        
        connection.close()
        stats = 'STATS - Total processed movies: ' + str(numMovies) + '. Added: ' + str(addedCounter) + '. Updated: ' + str(updatedCounter) + '. Errors: ' + str(processedKo) + ' (' + ('%.2f' % (0)) + '%)' + '. Deleted: ' + str(removedCounter)
        print utils.getCurrentTimeStamp() + ' - finished LEDDB movies updater in ' + self.hostName + ' ( ' + str(int(time.time()-tstart)) + ' seconds). ' + stats