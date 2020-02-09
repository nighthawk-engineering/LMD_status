#
#
######################################################################

import sys
import re
import json as js 
import subprocess as sp
from collections import OrderedDict
from sortedcontainers import SortedDict
import MySQLdb as sql

######################################################################
def transpose_dict(inp):
  out = OrderedDict()
  for ka,va in inp.items():
    for kb,vb in va.items():
      try:
        out[kb][ka] = vb
      except KeyError:
        out[kb] = OrderedDict()
        out[kb][ka] = vb
  #
  return(out)

######################################################################
def get_platform():
  platforms = {
    'linux'  : 'Linux',
    'darwin' : 'OS X',
    'win32'  : 'Windows'
  }
  if sys.platform not in platforms:
    return sys.platform
  
  return platforms[sys.platform]

######################################################################
def get_lic_data(server):
  platform = get_platform()
  if platform == 'Windows':
    lmutil = 'C:\\Users\\kschmahl\\Documents\\bin\\lmstatus\\lmutil'
  elif platform == 'Linux':
    lmutil = '/usr/local/cad/ansys/license_admin/scl/v11_linux64/lmutil'
  else:
    print('Unknown platform',platform)
    exit(1)

  lmstat = ('lmstat', '-a', '-c '+server)
  lmjob = lmutil + " " + " ".join(lmstat)
  #print(lmjob); #exit(1)

  lmstat_out = sp.getoutput(lmjob).splitlines()
  #print(js.dumps(lmstat_out, indent=2)); #exit(1)

  db = parseLMD(lmstat_out)
  #print(js.dumps(db, indent=2)); #exit(1)
  
  print('SERVER %s done, %s@%s for %s'%(server,db['LMPORT'],db['SERVER'],db['TOOL_LMD']))

  return(db)

######################################################################
def parseLMD(lmstat_out):
  db = OrderedDict()
  db['DATA'] = []

  in_server = False
  in_vendor = False
  in_tool   = False
  #
  for n,line in enumerate(lmstat_out):
    if len(line)==0:
      continue
    #
    regex1 = re.search('^License server status: ([^@]+)@(.*)',line)
    regex2 = re.search('^Error getting status: (.*)',line)
    if regex1:
      in_server = True
      in_tool = False
      #print(regex1.group(0))
      Server = regex1.group(2); Port = regex1.group(1)
      #print('Server: %s, Port: %s'%(Server,Port))
      db['SERVER'] = Server
      db['LMPORT'] = Port
      #print(db['LMPORT']+"@"+db['SERVER'])
      continue
    if regex2:
      print('Error getting status: '+regex2.group(1),
        file=sys.stderr)
      db['ERROR'] = regex2.group(1)
      break
    if in_server:
      regex = re.search(Server+': license server (\S+)',line)
      if regex:
        Server_status = regex.group(1)
        #print(regex.group(0))
        #print('Server %s is %s'%(Server,Server_status))
        db['SERVER_STATUS'] = Server_status
        in_server = False
        continue
    #
    regex = re.search('^Vendor daemon status',line)
    if regex:
      in_vendor = True
      continue
    if in_vendor:
      regex = re.search('([^ :]+): (\S+)',line)
      if regex:
        tool_lmd = regex.group(1)
        lmd_status = regex.group(2)
        #print('Ansyslmd status: ' + lmd_status)
        db['TOOL_LMD'] = tool_lmd
        db['LMD_STATUS'] = lmd_status
        #print('TOOL_LMD '+db['TOOL_LMD'])
        in_vendor = False
        continue
    #
    regex = re.search('^Users of ([^:]+)',line)
    if regex:
      db['DATA'].append(OrderedDict())
      data = db['DATA'][-1]
      in_tool = True
    #
    regex1 = re.search('^Users of ([^:]+):\s+\(Total of (.+) licenses? issued;\s+Total of (.+) licenses? in use\)',line)
    regex2 = re.search('^Users of ([^:]+):\s+\(Error: (.+) licenses?, unsupported by licensed server\)',line)
    regex3 = re.search('^Users of ([^:]+):\s+\((.*)\)',line)
    if regex1:
      license = regex1.group(1)
      total = regex1.group(2)
      used = regex1.group(3)
      #print('%s: %s of %s'%(license,used,total))
      data['LICENSE'] = license
      data['TOTAL'] = total
      data['USED'] = used
      data['USERS'] = []
      users = data['USERS']
    elif regex2:
      license = regex2.group(1)
      total = regex2.group(2)
      used = 'unsupported'
      #print('%s: %s license(s) unsupported'%(license,total))
      data['LICENSE'] = license
      data['TOTAL'] = total
      data['USED'] = used
      data['USERS'] = []
      users = data['USERS']
    elif regex3:
      license = regex3.group(1)
      total = '0'
      used = regex3.group(2)
      used = re.sub(' *, *',' - ',used)
      #print('%s: "%s"'%(license,used))
      data['LICENSE'] = license
      data['TOTAL'] = total
      data['USED'] = used
      data['USERS'] = []
      users = data['USERS']
    if in_tool:
      regex1 = re.search('^ {2}"([^"]+)".*expiry: (.*)',line)
      regex2 = re.search('^ {4}([^ ]+) ([^ ]+) ([^ ]+) (\d+)? ?(\([^)]+\)) ([^,]+), start ([^,]+)(,\s+(\d+)\s+licenses)?',line)
      if regex1:
        tool = regex1.group(1)
        expiry = regex1.group(2)
        #print('Tool %s expires %s'%(tool,expiry))
        data['EXPIRY'] = expiry
      if regex2:
        user    = regex2.group(1)
        mach1   = regex2.group(2)
        mach2   = regex2.group(3)
        unk     = regex2.group(4) if regex2.group(4) != None else 'nil'
        ver     = regex2.group(5)
        port    = regex2.group(6)
        started = regex2.group(7)
        numlic  = regex2.group(9) if regex2.group(9) != None else '1'
        #print('%s %s %s %s %s started %s'%(user,mach1,mach2,ver,port,started))
        users.append(OrderedDict())
        users[-1]['USER'] = user
        users[-1]['MACH1'] = mach1
        users[-1]['MACH2'] = mach2
        users[-1]['UNK'] = unk
        users[-1]['VER'] = ver
        users[-1]['PORT'] = port
        users[-1]['STARTED'] = started
        users[-1]['NUMLIC'] = numlic
    #
  return(db)

######################################################################
def db_dump(time,db):
  #print('Keys:',",".join(reversed(list(db.keys())))); exit(1)
  
  lstat = OrderedDict()
  lstat['TIME'] = time
  lmhdr = []
  lmlog = []
  
  for k in db.keys():
    if k != 'DATA':
      lstat[k] = db[k]
  
  data = db['DATA']
  for tool in data: # data = [ tools { license { user } } ]
    tstat = OrderedDict()
    for k in tool.keys():
      if k != 'USERS':
        tstat[k] = tool[k]
    #
    if len(tool['USERS']) == 0:
      #print('no USER data')
      hdrtmp = 'Keys: '+','.join(list(lstat.keys())+list(tstat.keys()))
      if (len(lmhdr) == 0) or (len(lmhdr[0]) < len(hdrtmp)):
        lmhdr = [hdrtmp]
      lmlog.append('Vals: "'+'","'.join(list(lstat.values())+list(tstat.values()))+'"')
    else:
      #print('USER data found')
      ustat = OrderedDict()
      for usr in tool['USERS']:
        for k in usr.keys():
          ustat[k] = usr[k]
        hdrtmp = 'Keys: '+','.join(list(lstat.keys())+list(tstat.keys())+list(ustat.keys()))
        if (len(lmhdr) == 0) or (len(lmhdr[0]) < len(hdrtmp)):
          lmhdr = [hdrtmp]
        lmlog.append('Vals: "'+'","'.join(list(lstat.values())+list(tstat.values())+list(ustat.values()))+'"')
  #
  if 'ERROR' in db:
    return(['ERROR: '+db['ERROR']])
  else:
    return(lmhdr+lmlog)

######################################################################
def make_rec(db):
  rec = SortedDict()
  #
  for line in db:
    parts = re.search('([^:]+): (.*)',line)
    label = parts.group(1)
    field = parts.group(2)
    fields = field.split(',')
    #print('make_rec',label,','.join(fields))
    #return([label,fields])
    #
    if label == 'Keys':
      rec['KEYS'] = fields
    elif label == 'Vals':
      try:
        rec['VALS'].append(fields)
      except KeyError:
        rec['VALS'] = []
        rec['VALS'].append(fields)
    elif label == 'ERROR':
      rec['ERROR'] = field
    else:
      print('Unexpected record found in \'make_rec\':\n'+line)
      exit(1)
    #
  return(rec)

######################################################################
def put_rec(dbh,rec,table):
  #print('in put_rec')
  c = dbh.cursor()
  #print('MySQL cursor',c)
  
  fields = rec['KEYS']
  values = rec['VALS']
  #
  def make_cols(items):
    cols = '('+','.join(items)+')'
    return(cols)

  def make_vals(items):
    vals = '('+','.join(items)+')'
    return(vals)

  insert = 'INSERT INTO %s '%table

  exec_log = []
  fetch_log = []
  commit = None
  for line in values:
    numcol = len(line)
    cols = fields[:numcol]
    
    cmd = insert + make_cols(cols) + ' VALUES ' + make_vals(line) + ';'
    #print(cmd)
    
    exec_log.append(str(c.execute(cmd)))
    #fetch_log.append(c.fetchall())
    
  print('Execute:',','.join(exec_log))
  #print('FetchAll:\n','\n'.join(fetch_log))
  commit = dbh.commit()
  print('Commit:'+str(commit))

######################################################################
def get_rec(dbh,query,distinct=False):
  c = dbh.cursor()
  #print('MySQL cursor',c)

  table = query['TABLE']
  unique = 'DISTINCT ' if distinct else ''
  where = ' WHERE '+query['WHERE'] if 'WHERE' in query.keys() else ''
  
  cmd = 'SELECT '+unique+','.join(query['FIELDS'])+' FROM '+table+where+';'
  c.execute(cmd)
  rec = c.fetchall()
  #print(js.dumps(rec, indent=2)); #exit(1)

  return(rec)

######################################################################
def sql_login():
  dbh = sql.connect(
    host='redrock',
    user='chipcharuser',
    passwd='2UK2tNpV.1G',
    db='chipchar'
  )
  #print(dbh); exit(1)
  #print('MySQL login successful')
  
  return(dbh)

######################################################################
######################################################################
