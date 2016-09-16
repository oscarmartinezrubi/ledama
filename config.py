#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################

# This file contains the configuration of the ledama project. You must change this lines
# depending on the location of the software

##### PATHS #####

LEDAMA_ABS_PATH = '/home/users/lofardata/martinez/software/ledama'
HOME_PARENT_PATH = '/home/users'
DEF_CLUSTER_DESCRIPTION_FILE = '/software/users/lofareor/clustdesc/eor-calc.desc'
FDT_PATH = '/home/users/lofardata/martinez/software/fdt.jar'
GRID_INIT_FILE = '/home/users/lofardata/martinez/gridinit.sh'

##### LMODULES ######
INIT_FILE = '/software/users/lofarsoft/software/lofarsoft.sh'
FULL_ACCESS_USERS = ['lofardata',]
WEB_USERS = ['leddbweb',]
ENABLED_LMODULES_PATHS_WEB_USERS = ['/datamanagement/modules', '/datamanagement/modules/pipeline', '/dataanalysis/modules/ascii', '/dataanalysis/modules/plot',]
ENABLED_LMODULES_PATHS_USERS = ENABLED_LMODULES_PATHS_WEB_USERS + ['/leddb/modules/get','/nodes/modules',]
ENABLED_LMODULES_PATHS_FULL_ACCESS_USERS = ENABLED_LMODULES_PATHS_USERS + ['/datamanagement/modules/archive/fdt', '/datamanagement/modules/archive/grid','/datamanagement/modules/archive/sip','/leddb/modules','/leddb/modules/edit','/leddb/modules/update','/leddb/modules/add']

##### LEDDB #####

LEDDB_NAME = 'leddb'
LEDDB_HOST = 'node001'
LEDDB_LOGS_FOLDER = '/home/users/lofardata/martinez/logs/leddbupdate'
LEDDB_BACKUP_HOST = 'node080' # This must be configured by setting the cron task in this node
LEDDB_BACKUP_FOLDER = '/data3/users/lofareor/leddb_backup'
# Number of MS referenced in each partition (the DIAG tables have partitions )
LEDDB_MS_PART = 20000
# Show queries
DEBUG = False
# TARGET UPDATING
TARGET_FDT_UPDATE_FOLDER = '/home/users/lofardata/martinez/logs/leddbupdate/TARGET_FDT'
TARGET_FDT_DIR_LIST_FILE = 'target_eor_root_dir.txt'
TARGET_FDT_FILE_LIST_FILE = 'filelist.txt'
# LEDDB web (which should be run by the web user, i.e. leddbweb)
LEDDB_WEB_DIR = '/home/users/leddbweb/web'

##### CLUSTER MONITOR DB #####

NODE_MONITOR_FOLDER = '/home/users/lofardata/nodesmonitor/'
# POSSIBLE TYPES ARE: db and file
NODE_MONITOR_TYPE = 'db'
NODE_MONITOR_DB_NAME = 'nmdb'
NODE_MONITOR_DB_HOST = 'node078'

##### PLOTTING #####

MATPLOTLIB_BACKEND = 'Agg'
