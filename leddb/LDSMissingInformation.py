#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################

# This is just a module containing the missing information for the LDSs (Project, Field, Beam)

# This is the dictioanry containing the information, the keys are the LXXXXX and the infro is a 3 strings tuple, Project, Field ,Beam 
dictionary = {}

dictionary['L21478'] = ( 'LEA102',      'MOON',             'LBA_OUTER')
dictionary['L21479'] = ( 'LEA102',      'MOON',             'LBA_OUTER')
dictionary['L21480'] = ( 'LEA102',      'MOON',             'HBA_DUAL')
dictionary['L21482'] = ( 'LEA102',      'MOON',             'HBA_DUAL')    
dictionary['L22174'] = ( 'LOFAROPS',    'NCP',              'HBA_ZERO')    
dictionary['L24691'] = ( 'no campaign', 'Cassiopeia A',     'LBA_OUTER') 
dictionary['L20979'] = ( 'LEA128',      'EMPTY',            'HBA_DUAL')    
dictionary['L20980'] = ( 'LEA128',      'NCP',              'HBA_ZERO')
dictionary['L20981'] = ( 'LEA128',      '3C196',            'HBA_ZERO')
dictionary['L20982'] = ( 'LEA128',      'EMPTY',            'HBA_ZERO')
dictionary['L20983'] = ( 'LEA128',      'NCP',              'HBA_ZERO')
dictionary['L20984'] = ( 'LEA128',      '3C196',            'HBA_ZERO')
dictionary['L22005'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L22006'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L22007'] = ( 'LEA128',      'EMPTY',            'HBA_DUAL')
dictionary['L22059'] = ( 'LEA102',      'MOON',             'HBA_DUAL')
dictionary['L22060'] = ( 'LEA102',      '3C123',            'HBA_DUAL')
dictionary['L22061'] = ( 'LEA102',      'MOON',             'LBA_OUTER')
dictionary['L22062'] = ( 'LEA102',      '3C123',            'LBA_OUTER')
dictionary['L22120'] = ( 'MSSS',        'HerA',             'LBA_OUTER')
dictionary['L22467'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L22666'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L22667'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L22866'] = ( 'LEA102',      'MOON',             'HBA_DUAL')
dictionary['L23090'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L23092'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L23259'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L23260'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L23571'] = ( 'LEA128',      '3C196',            'LBA_OUTER')
dictionary['L23572'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L23573'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L23756'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L24380'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L24560'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L24561'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L24836'] = ( 'LEA128',      'NCP',               None)
dictionary['L24837'] = ( 'LEA128',      '3C196',             None)
dictionary['L23755'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L23625'] = ( 'LEA128',      '3C196',            'HBA_DUAL')
dictionary['L23624'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L23377'] = ( 'LEA128',      'NCP',              'HBA_DUAL')
dictionary['L22766'] = ( 'LEA128',      '3C196',            'HBA_DUAL')


def getProject(ldsName):
    try: 
        return dictionary[ldsName][0]
    except:
        return None
    
def getField(ldsName):
    try: 
        return dictionary[ldsName][1]
    except:
        return None
    
def getAntennaSet(ldsName):
    try: 
        return dictionary[ldsName][3]
    except:
        return None
