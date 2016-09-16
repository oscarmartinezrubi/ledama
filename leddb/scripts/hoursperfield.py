#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Connector import Connector

MIN_UTC = '2012/11/29'

# This script computes the number of obs. hours fir each of the main fields (excluding calibrators, i.e. LDS which duration is less than 5 minutes)
# It requires a MIN_UTC

def showField(fieldname, value):
    if value != None:
        print fieldname + ' observing hours: %.2f' % float(value)
    else:
        print fieldname + ' observing hours: 0'

connection = Connector().getConnection()
cursor = connection.cursor()

cursor.execute("select (sum(LDS_duration)/3600.) as duration_hours FROM (select LDS_DURATION from lofar_dataset, lofar_dataset_meta where lofar_dataset.name = lofar_dataset_meta.lofar_dataset_meta_id and initial_utc > '" + MIN_UTC + "' and LDS_DURATION > 300 and LDS_MAIN_FIELD ~ 'ELAIS' order by initial_utc) AS SELECTED")
showField('ELAIS', cursor.fetchone()[0])

cursor.execute("select (sum(LDS_duration)/3600.) as duration_hours FROM (select LDS_DURATION from lofar_dataset, lofar_dataset_meta where lofar_dataset.name = lofar_dataset_meta.lofar_dataset_meta_id and initial_utc > '" + MIN_UTC + "' and LDS_DURATION > 300 and LDS_MAIN_FIELD ~ 'NCP' order by initial_utc) AS SELECTED")
showField('NCP', cursor.fetchone()[0])

cursor.execute("select (sum(LDS_duration)/3600.) as duration_hours FROM (select LDS_DURATION from lofar_dataset, lofar_dataset_meta where lofar_dataset.name = lofar_dataset_meta.lofar_dataset_meta_id and initial_utc > '" + MIN_UTC + "' and LDS_DURATION > 300 and (LDS_MAIN_FIELD ~ '3C196' or LDS_MAIN_FIELD ~ '3C 196') order by initial_utc) AS SELECTED")
showField('3C196', cursor.fetchone()[0])

cursor.execute("select (sum(LDS_duration)/3600.) as duration_hours FROM (select LDS_DURATION from lofar_dataset, lofar_dataset_meta where lofar_dataset.name = lofar_dataset_meta.lofar_dataset_meta_id and initial_utc > '" + MIN_UTC + "' and LDS_DURATION > 300 and (LDS_MAIN_FIELD ~ '3C295' or LDS_MAIN_FIELD ~ '3C 295') order by initial_utc) AS SELECTED")
showField('3C295', cursor.fetchone()[0])

cursor.execute("select (sum(LDS_duration)/3600.) as duration_hours FROM (select LDS_DURATION from lofar_dataset, lofar_dataset_meta where lofar_dataset.name = lofar_dataset_meta.lofar_dataset_meta_id and initial_utc > '" + MIN_UTC + "' and LDS_DURATION > 300 and (LDS_MAIN_FIELD ~ 'MO' or LDS_MAIN_FIELD ~ 'mo' or LDS_MAIN_FIELD ~ 'Mo') order by initial_utc) AS SELECTED")
showField('MOON', cursor.fetchone()[0])

cursor.execute("select (sum(LDS_duration)/3600.) as duration_hours FROM (select LDS_DURATION from lofar_dataset, lofar_dataset_meta where lofar_dataset.name = lofar_dataset_meta.lofar_dataset_meta_id and initial_utc > '" + MIN_UTC + "' and LDS_DURATION > 300 and (LDS_MAIN_FIELD !~ 'MO' and LDS_MAIN_FIELD !~ 'mo' and LDS_MAIN_FIELD !~ 'Mo' and LDS_MAIN_FIELD !~ '3C295' and LDS_MAIN_FIELD !~ '3C 295' and LDS_MAIN_FIELD !~ '3C196' and LDS_MAIN_FIELD !~ '3C 196' and LDS_MAIN_FIELD !~ 'NCP' and LDS_MAIN_FIELD !~ 'ELAIS') order by initial_utc) AS SELECTED")
showField('OTHERS', cursor.fetchone()[0])