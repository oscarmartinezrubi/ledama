#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
        
# This module define some operations to be used for the nodes

# Get the paths of LOFARDataSets found in pathToCheck. 
# It will consider valid paths the folders starting with L
def getLDSPaths(pathToCheck):
    ldsPaths = []
    if os.path.isdir(pathToCheck):
        for subPathToCheck in os.listdir(pathToCheck):
            if subPathToCheck.startswith('L'):
                ldsPaths.append(os.path.abspath(pathToCheck) + '/' + subPathToCheck)
    return ldsPaths

# Get a list of the absPaths of the MSs from pathToCheck
def getMSsFromPath(pathToCheck):
    absPaths=[]
    lowerPathToCheck = pathToCheck.lower()
    if os.path.isdir(pathToCheck):
        if lowerPathToCheck.count('sb') and (lowerPathToCheck.endswith('ms') or lowerPathToCheck.endswith('dppp')):
            absPaths.append(pathToCheck)
        else:
            try:
                listPathToCheckContents = sorted(os.listdir(pathToCheck), key=str.lower)
            except:
                listPathToCheckContents = []
            for subPath in listPathToCheckContents:
                absPaths.extend(getMSsFromPath(os.path.abspath(pathToCheck) + '/' + subPath))
    elif os.path.isfile(pathToCheck) and lowerPathToCheck.count('sb') and (lowerPathToCheck.endswith('tar') or lowerPathToCheck.endswith('bvf')) :
        # We also add to the list the TAR files
        absPaths.append(pathToCheck)
    return absPaths  