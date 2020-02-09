##!/share/dev/tools/64/bin/python3.5

######################################################################

import os
import sys
import re
import csv
import json as js 
import subprocess as sp
from collections import OrderedDict
from datetime import datetime
import multiprocessing as mp
import MySQLdb as sql
from LMSTAT_snapshot2 import *

######################################################################
######################################################################
if __name__ == '__main__':

  dt = datetime.now().strftime('20%y%m%d-%H%M%S')
  print('\nLMstat -- Time:',dt); #exit(1)

  #####################################
  # Lets get a list of servers and licenses
  query = {}
  query['TABLE'] = 'LMSERVERS'
  query['FIELDS'] = ['VALID', 'LMPORT', 'SERVER']

  dbh = sql_login()
  data = get_rec(dbh,query)
  dbh.close()
  
  servers = []
  for (valid, port, server) in data:
    if valid:
      servers.append(port+'@'+server)
  #print('\n'.join(servers)); exit(0)
  
  # servers = [
    # "1717@cad2",
    # "1717@licenseserver",
    # "1055@idukki",
    # "1881@idukki",
    # "27000@blrfe2",
    # "27000@cad1",
    # "27001@licenseserver",
    # "27002@idukki",
    # "27005@cad1",
    # "27009@cad2", # end of good servers
    # "1709@licenseserver",
    # "27000@fe13",
    # "27000@fe28",
    # "5219@licenseserver",
    # "5280@cad1",
    # "7009@licenseserver" # end of bad servers
  # ]

  #PROCESSES = 4
  PROCESSES = len(servers)

  #####################################

  platform = get_platform()
  if platform == 'Windows':
    lmutil = 'C:\\Users\\kschmahl\\Documents\\bin\\lmstatus\\lmutil'
  elif platform == 'Linux':
    lmutil = '/usr/local/cad/ansys/license_admin/scl/v11_linux64/lmutil'
  else:
    print('Unknown platform',platform)
    exit(1)
  #print(lmutil); #exit(1)

  #####################################

  with mp.Pool(PROCESSES) as p:
    dbs = p.map(get_lic_data, servers)
  
  #####################################

  licenses=OrderedDict()
  dumps = []; recs = []
  for srvr,db in zip(servers,dbs):
    if 'TOOL_LMD' in db:
      dumps.append(db_dump(dt,db))
      recs.append(make_rec(dumps[-1]))
      #
      try:
        licenses[db['TOOL_LMD']].append(srvr)
      except KeyError:
        licenses[db['TOOL_LMD']] = []
        licenses[db['TOOL_LMD']].append(srvr)
  
  #####################################
  
  print(js.dumps(dbs,   indent=2)); exit(1)
  #print(js.dumps(dumps, indent=2)); exit(1)
  #print(js.dumps(recs,  indent=2)); exit(1)
  #print(js.dumps(licenses, indent=2)); exit(1)
  
  #####################################
  
  #exit(1)
  table = 'LMSTAT'
  dbh = sql_login()
  for rec in recs:
    if 'KEYS' in rec:
      print(','.join(rec['KEYS']))
      #put_rec(dbh,rec,table)
  dbh.close()








