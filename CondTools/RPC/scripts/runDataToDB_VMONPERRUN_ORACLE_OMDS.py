"""
runDataToDB
Author: Algirdas Beinaravicius
"""
import os
import optparse
import sys
#sys.path.append("/nfshome0/mmaggi/PYTHON")
import datetime
import re
is_cx_Oracle = True
is_sqlalchemy = True
try:
  import cx_Oracle
except Exception, e:
  is_cx_Oracle = False

try:
  import sqlalchemy
except Exception, e:
  is_sqlalchemy = False
  print "no sqalchemy!!!"

class DBService(object):
#  def __init__(self, connectionService):
#    self.__engine = connectionService.engine()
#    self.__connection = connectionService.engine.connect()
#    self.__connection = connectionService.connection()


  def __init__(self, engine):
    self.__engine = engine
    self.__schema = "CMS_COND_RPC_NOISE"
  #    self.__connection = connection

  #  def __init__(self, engine):
  #    self.__engine = engine

  def createDBPERRUN(self):
    sqlalchemy.databases.oracle.OracleNumeric.precision=10
    metadata = sqlalchemy.MetaData()
    generalRuns = sqlalchemy.Table('RPCVMONPERRUN', metadata,
                                   sqlalchemy.Column('RUN_NUMBER', sqlalchemy.Integer, primary_key = True, nullable=False ),
                                   sqlalchemy.Column('DPID', sqlalchemy.Integer, primary_key = True, nullable=False ),
                                   sqlalchemy.Column('STATUS',sqlalchemy.Integer),
                                   sqlalchemy.Column('VMON', sqlalchemy.Numeric(precision=10,length=8)),
                                   sqlalchemy.Column('VMONRPCON', sqlalchemy.Numeric(precision=10,length=8)),
                                   sqlalchemy.Column('VMONCMSRPCON', sqlalchemy.Numeric(precision=10,length=8)),
                                   sqlalchemy.PrimaryKeyConstraint('RUN_NUMBER','DPID',name="RPCVMONPERRUN_PK"),
                                   #    sqlalchemy.PrimaryKeyConstraint('DPID',name="DPID_PK"),
                                   schema = self.__schema                               
                                )
    #    sqlalchemy.Index('RPCVMONPERRUN_RUN_NUMBER_IX', dqmRuns.c.RUN_NUMBER),

    generalRuns.create(self.__engine)

    #    sqlalchemy.Index('RPCGENERAL_RUN_NUMBER_IX', generalRuns.c.RUN_NUMBER),
    #  def createDBPERLUMI(self):
#    sqlalchemy.databases.oracle.OracleNumeric.precision=10
#    metadata = sqlalchemy.MetaData()
    dqmRuns = sqlalchemy.Table('RPCVMONPERLUMI', metadata,
                               #sqlalchemy.Column('RUN_NUMBER', sqlalchemy.Integer,sqlalchemy.ForeignKey("RPCVMONPERRUN.RUN_NUMBER",name="RPCVMONPERLUMI_VMONPERRUN_FK1"),primary_key= True, nullable=False),
                               sqlalchemy.Column('RUN_NUMBER', sqlalchemy.Integer, primary_key = True, nullable=False ),
                               sqlalchemy.Column('LUMI',sqlalchemy.Integer,primary_key = True,nullable = False),
                               #    sqlalchemy.Column('DPID', sqlalchemy.Integer,sqlalchemy.ForeignKey("RPCVMONPERRUN.DPID",name="RPCVMONPERLUMI_VMONPERRUN_FK2"),primary_key= True, nullable=False),
                               sqlalchemy.Column('DPID',sqlalchemy.Integer,primary_key = True,nullable=False),
                               sqlalchemy.Column('STATUS',sqlalchemy.Integer),
                               sqlalchemy.Column('VMON', sqlalchemy.Numeric(precision=10,length=8)),
                               #    sqlalchemy.Column('eff_seg_t', sqlalchemy.Float(precision=10)),
                               sqlalchemy.PrimaryKeyConstraint('RUN_NUMBER','LUMI','DPID',name="RPCVMONPERLUMI_PK"),                                                         #    sqlalchemy.PrimaryKeyConstraint('RUN_NUMBER',name="RPCVMONPERRUN_PK"),
                               #    sqlalchemy.PrimaryKeyConstraint('DPID',name="DPID_PK"),
                               #    schema = self.__schema                               
#                               schema = self.__schema
                               )
    sqlalchemy.Index('RPCVMONPERRUN_RUN_NUMBER_IX', dqmRuns.c.RUN_NUMBER),
    #    sqlalchemy.Index('RPCVMONPERLUMI_RUN_NUMBER_IX', dqmRuns.c.RUN_NUMBER),

    dqmRuns.create(self.__engine)


    #metadata.create_all(self.__engine)
    
#  def deleteFromDB(self, runNumber=None):
#    metadata = sqlalchemy.MetaData()
#    table = sqlalchemy.Table('RPCDQMNEW', metadata, 
#    autoload=True, autoload_with=self.__engine)
    
    #delete all rows with given runnumber from the data base
#    connection = self.__connection
#    delete = table.delete().where(table.c.runnumber==runNumber)
#    connection.execute(delete)
#    connection.close()
            
  def insertToDB(self, dataListVMONPERRUN, dataListVMONPERLUMI):
    metadata = sqlalchemy.MetaData()
    insertList = []

    tableVMONPERRUN = sqlalchemy.Table('RPCVMONPERRUN', metadata, #schema = self.__schema,
    autoload=True, autoload_with=self.__engine)
    for data in dataListVMONPERRUN:
#      print " runid 1 is " + data['run_number']
      ins = tableVMONPERRUN.insert().values( run_number = data['RUN_NUMBER'],
                                   # exec_date = data['date'],
                                   dpid = data['DPID'],
                                   status = data['STATUS'],
                                   imon = data['VMON'], 
                                   imon = data['VMONRPCON'], 
                                   imon = data['VMONCMSRPCON'], 
                                   )
      insertList.append(ins)
    if (not dataListVMONPERLUMI == None)  :
      tableVMONPERLUMI = sqlalchemy.Table('RPCVMONPERLUMI', metadata, #schema = self.__schema,
                                          autoload=True, autoload_with=self.__engine)
      for data in dataListVMONPERLUMI:
        print " runid is " + data['RUN_NUMBER']
        ins = tableVMONPERLUMI.insert().values( run_number=data['RUN_NUMBER'],
                                                lumi =data['LUMI'],
                                                dpid=data['DPID'],
                                                status = data['STATUS'],
                                                imon = data['VMON'], 
                                                
                                                #eff_seg_t=data['eff_seg_t'],
                                                )
        insertList.append(ins)

    return insertList  
#      connection.execute(ins)   
#    connection.close()

class FileListRetriever(object):
  def __init__(self):
    pass
  
  #retrieves a list of files from run****, which is placed in
  def retrieveList(self, runDir):
    fileList = [os.path.join(runDir, f) for f in os.listdir(runDir) if os.path.isfile(os.path.join(runDir, f))]
    return fileList



class FileReaderVMONPERLUMI(object):
  def __init__(self):
      #header of the file should contain only the run number
    self.__headerPattern ='[0-9]+$' #'^[\+\-0-9]+[ \t]+[0-9]+$'
    
    #-1 : placeholder
    self.__runDataPatternOff = """
      ^
      ([0-9]+[ \t]+){2}
      $
      """
    #Contains : lumi, dpid (2 int), imon (float), status
    self.__runDataPattern = """
      ^
      ([\+\-0-9]+(\.*[0-9]+){0,1}([eE][+-]?\d+)?[ \t]+){3} 
      [+-]?((\d+(\.\d*)?)|\.\d+)([eE][+-]?[0-9]+)?
      $
      """

      
  #checks if the file structure and its contents are correct
  def __contentCheck(self, contents):
    contentMeta = {'correct' : True, 'errors' : []}
    
    #check header length
    if not re.match(self.__headerPattern, contents[0]):
      contentMeta['correct'] = False
      contentMeta['errors'].append('File header (first line of the file) is incorrect')
    
    for run in contents[1:]:
      if not re.match(self.__runDataPattern, run, re.VERBOSE):
        if not re.match(self.__runDataPatternOff, run, re.VERBOSE):
          contentMeta['correct'] = False
          contentMeta['errors'].append('run data ' + run + ' is incorrect')
    return contentMeta
    
  def read(self, fileName):
    f = open(fileName, 'r')
    contents = f.read().strip().split('\n')
    
    #check if file content is correct
    contentMeta = self.__contentCheck(contents)
    if not contentMeta['correct']:
      print 'ERROR:\t' + fileName + ' content is incorrect'
      for error in contentMeta['errors']:
          print '\t' + error
      sys.exit(-1)
      
    #first line of a run file is a header
    header = contents[0].split()
    runID = header[0]
#    timeStamp = header[1]
#    timeStamp = datetime.datetime.fromtimestamp(float(header[1]))
    #following lines of a file are data
    runList = []
    for RUN in contents[1:]:
      data = RUN.split()
      runData = {}
      runData['RUN_NUMBER'] = runID
#runData['date'] = timeStamp
      runData['DPID'] = data[0]
      runData['LUMI'] = data[1]
      runData['VMON'] = data[2]
      runData['VMONRPCON'] = data[3]
      runData['VMONCMSRPCON'] = data[4]
      runData['STATUS'] = data[5]
#      runData['eff_seg_t'] = data[7]
      runList.append(runData)
    return runList

class FileReaderVMONPERRUN(object):
  def __init__(self):
      #header of the file should contain only the run number
    self.__headerPattern ='[0-9]+$' #'^[\+\-0-9]+[ \t]+[0-9]+$'
    
    #-1 : placeholder
    self.__runDataPatternOff = """
      ^
      ([0-9]+[ \t]+){1}
      $
      """
    #Contains : roll raw id (int) , n_digis (int), number of clusters, clustersize, bx, bx rms, efficiency (5 floats)
    #and status of the roll (int), separated by white spaces


    self.__runDataPattern = """
      ^
      ([0-9]+[ \t]+)
      ([\+\-0-9]+(\.*[0-9]+){0,1}([eE][+-]?\d+)?[ \t]+){3}
      [+-]?((\d+(\.\d*)?)|\.\d+)([eE][+-]?[0-9]+)?
      $
      """
      
  #checks if the file structure and its contents are correct
  def __contentCheck(self, contents):
    contentMeta = {'correct' : True, 'errors' : []}
    
    #check header length
    if not re.match(self.__headerPattern, contents[0]):
      contentMeta['correct'] = False
      contentMeta['errors'].append('File header (first line of the file) is incorrect')
    
    for run in contents[1:]:
      if not re.match(self.__runDataPattern, run, re.VERBOSE):
        if not re.match(self.__runDataPatternOff, run, re.VERBOSE):
          contentMeta['correct'] = False
          contentMeta['errors'].append('run data ' + run + ' is incorrect')
    return contentMeta
    
  def read(self, fileName):
    f = open(fileName, 'r')
    contents = f.read().strip().split('\n')
    
    #check if file content is correct
    contentMeta = self.__contentCheck(contents)
    if not contentMeta['correct']:
      print 'ERROR:\t' + fileName + ' content is incorrect'
      for error in contentMeta['errors']:
          print '\t' + error
      sys.exit(-1)
      
    #first line of a run file is a header
    header = contents[0].split()
    print "header is " +header[0]
    runID = header[0]
#    timeStamp = header[1]
#    timeStamp = datetime.datetime.fromtimestamp(float(header[1]))
    #following lines of a file are data
    runList = []

    for run in contents[1:]:
      data = run.split()
      runData = {}
      runData['RUN_NUMBER'] = runID
#      print " runid is " + runID

      runData['DPID'] = data[0]
      runData['VMON'] = data[1]
      runData['STATUS'] = data[2]
      runList.append(runData)
    return runList

    
class ArgumentParser(object):
  def __init__(self):
    self.__parser = optparse.OptionParser()
    usageStr = 'Usage: ' + sys.argv[0] + ' [options] fileName.txt\n'
    usageStr += 'Example: \n'
    usageStr += sys.argv[0] + ' -s sqliteDB.db fileName.txt\n'
    usageStr += sys.argv[0] + ' -o oracleSID -u userName -p password --host=hostname --port=port fileName.txt\n'
    usageStr += sys.argv[0] + ' -o oracleSID -u userName -p password --host=hostname --port=port runDir\n'
    usageStr += sys.argv[0] + ' -o oracleTNS -u userName -p password runDir\n'
    usageStr += sys.argv[0] + ' -o oracleTNS --updaterun runDir\n'
    
    
    self.__parser.set_usage(usageStr)
    self.__parser.add_option('-s', '--sqlite',
              dest='sqliteDB',
              type='string',
              action='store',
              help='sqlite database name')
    self.__parser.add_option('-o', '--oracle',
              dest='oracleDB',
              type='string',
              action='store',
              help='oracle sid or oracle tns_name')
    self.__parser.add_option('-u', '--user',
              dest='username',
              action='store',
              type='string',
              help='username to connect to the database')
    self.__parser.add_option('-p', '--password',
              dest='password',
              action='store',
              type='string',
              help='password to connect to the database')
    self.__parser.add_option('--host',
              dest='hostname',
              action='store',
              type='string',
              help='hostname of the database')
    self.__parser.add_option('--port',
              dest='port',
              action='store',
              type='string',
              help='connection port')
    self.__parser.add_option('--updaterun',
              dest='updaterun',
              default=False,
              action='store_true',
              help='specify this flag, if update on run is perfomed')
  def __checkArguments(self, options, remainder):
    if len(remainder) == 0:
      print 'Specify file or directory name. Use -h to get more help'
      sys.exit(-1)
    if options.sqliteDB:
      if options.oracleDB:
        print 'Only one database has to be specified. Use -h to get more help'
        sys.exit(-1)
    else:
      if (options.oracleDB is None):
        print 'Database not specified. Use -h to get more help'
        sys.exit(-1)
      else:
        if (not is_cx_Oracle):
          print 'ERROR!!!: cx_Oracle module not installed'
          sys.exit(-1)
        if not options.username:
          print 'Username not specified. Use -h to get more help'
          sys.exit(-1)
        if not options.password:
          print 'Password not specified. Use -h to get more help'
          sys.exit(-1)
      
  def parse(self, cmd_args):
    settings = {}
    options, remainder = self.__parser.parse_args(cmd_args)
    self.__checkArguments(options, remainder)
    settings['remainder'] = remainder[0]
    settings['username'] = None
    settings['password'] = None
    settings['hostname'] = None
    settings['port'] = None
    settings['updaterun'] = options.updaterun
    if options.sqliteDB:
      settings['dbType'] = 'sqlite:///'
      settings['dbName'] = options.sqliteDB
    elif options.oracleDB:
      settings['dbType'] = 'oracle://'
      settings['dbName'] = options.oracleDB
      settings['username'] = options.username
      settings['password'] = options.password
      settings['hostname'] = options.hostname
      settings['port'] = options.port
    return settings

"""
if __name__ == '__main__':
  if (not is_sqlalchemy):
    print 'ERROR: not able to import sqlalchemy module'
    sys.exit(-1)

  argParser = ArgumentParser()
  settings = argParser.parse(sys.argv[1:])


  dbService = DBService(dbType=settings['dbType'], host=settings['hostname'], port=settings['port'], user=settings['username'], password=settings['password'], dbName=settings['dbName'])
  dbService.createDB()
  fileReader = FileReader()
#  fileReader.read("190465_bis.txt")  
#  data = fileReader.read("196430.txt")  
  data = fileReader.read(settings['remainder'])
  dbService.insertToDB(data)
"""

#if __name__ == '__main__':
#  if (not is_sqlalchemy):
#    print 'ERROR: not able to import sqlalchemy module'
#    sys.exit(-1)
#    
#  argParser = ArgumentParser()
#  settings = argParser.parse(sys.argv[1:])
#  
#  dbService = DBService(dbType=settings['dbType'], host=settings['hostname'], port=settings['port'], user=settings['username'], password=settings['password'], dbName=settings['dbName'])
#  dbService.createDB()
#  fileReader = FileReader()
#  
#  if os.path.isdir(settings['remainder']): #user wants to insert multiple file data into database
#    fileListRetriever = FileListRetriever()
#    fileList = fileListRetriever.retrieveList(settings['remainder'])
#    deletePerformed = False
#    for f in fileList:
#      runList = fileReader.read(f)
#      #in case of update, deletion of rows from database has to be performed only once
#      if (settings['updaterun']) and (not deletePerformed):
#        dbService.deleteFromDB(runNumber=runList[0]['run'])
#        deletePerformed = True
#      dbService.insertToDB(runList)
#      print 'Inserted ' + f
#  else:
#    runList = fileReader.read(settings['remainder'])
#    dbService.insertToDB(runList)
#    print "run%s" %runList[0]['run']
#  print 'runDataToDB_strip.py: DONE Successfully'
#  sys.exit(0) 
