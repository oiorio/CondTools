"""
runDataToDB
Author: Algirdas Beinaravicius, modified A.O.M. Iorio 
"""


import os
import optparse
import sys
#sys.path.append("./PYTHON/PYTHON")
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

#from ConStrParser import extract
#import runDataToDB_quality_class_ORACLE
#import runDataToDB_DQM_GENERAL_ORACLE_NoSchema
import runDataToDB_PVSSPERRUN_ORACLE_OMDS
#runDataToDB_DQM_GENERAL_ORACLE_OMDS
#import runDataToDB_EFFICIENCY_ORACLE_OMDS


class DBConnectionService(object):
  def __init__(self, dbType='sqlite:///', host=None, port=None, user='', password= '', dbName='runData.db',option=''):
    self.__dbType = dbType
    self.__host = host
    self.__user = user
    self.__password = password
    self.__dbName = dbName
    self.__supportedDBs = {'sqlite':['sqlite://', 'sqlite:///'], 'oracle':['oracle://']}
    self.__isConnected =0
    if (dbType in self.__supportedDBs['sqlite']):
      self.__alchemyDBString = dbType + dbName 
    else:
      #create oracle connection string
      if host and port:
        self.__alchemyDBString = dbType + user + ':' + password + '@' + host + ':' + port + '/' + dbName
      else:
        self.__alchemyDBString = dbType + user + ':' + password + '@' + dbName
    self.__engine = sqlalchemy.create_engine(self.__alchemyDBString)
#    self.__engine.echo = True 
    self.__connection = self.__engine.connect()
#    self.__connection.echo = True 
    
  def engine(self):
    return self.__engine

  def connection(self):
    return self.__connection

  def isConnected(self):
    return self.__isConnected
    
  def setConnection(self,set):
    __isConnected = set


  def deleteFromDB(self, tableName, runNumber=None):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(tableName, metadata, 
    autoload=True, autoload_with=self.__engine)
    
    #delete all rows with given runnumber from the data base
    delete = table.delete()#.where(table.c.runnumber=runNumber)
    self.__connection.execute(delete)


  def dropFromDB(self, tableName):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(tableName, metadata, 
    autoload=True, autoload_with=self.__engine)
    table.drop(self.__engine)

  def insertToDB(self, insertName, runNumber=None):
    self.__connection.execute(ins)

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
     

runlistPVSSPERLUMI = []
#runlistPVSSPERLUMI.append(200091)
#runlistDQM.append(200049)
#runlistDQM.append(200041)
#runlistDQM.append(200042)

#runlistEFFICIENCY = []
#runlistEFFICIENCY.append(200091)
#runlistEFFICIENCY.append(200041)
#runlistEFFICIENCY.append(200042)

#from listTmpDQM import runs_in_db as runlistDQM
#from runs_pvss_ran import runs_in_db as runlistDQM
#from runs_pvss_to_run import runs_in_db as runlistDQM

from runs_db_ran_total import runs_in_db as runlistDQM
#from listTmpEFF import runs_in_db as runlistEFFICIENCY


directory = "/nfshome0/pugliese/oiorio/newTables/testpvss/"
directory = "./" +"CondTools/RPC/bin/"
directory = "../bin/"
directory = "/afs/cern.ch/work/o/oiorio/public/xPaola/newRuns/"

if __name__ == '__main__':

  print is_sqlalchemy
  if (not is_sqlalchemy):
    print 'ERROR: not able to import sqlalchemy module'
    sys.exit(-1)
    
  argParser = ArgumentParser()
  settings = argParser.parse(sys.argv[1:])
  schema = "CMS_RPC_COND"
  #  runlistDQM = fileReader.read(settings['remainder'])
  
  
#  dbConnectionService=DBConnectionService(settings['dbType'], host=settings['hostname'], port=settings['port'], user=usr, password=passwd, dbName=settings['dbName'])


  
  dbConnectionService = DBConnectionService(dbType=settings['dbType'], host=settings['hostname'], port=settings['port'], user=settings['username'], password=settings['password'], dbName=settings['dbName'])
    


  print "dbyup is " 
  print settings['dbType']

  if settings['dbType'] == "sqlite:///":
    import runDataToDB_PVSSPERRUN_SQL

  #dbConnectionService=DBConnectionService(settings['dbType'], host=settings['hostname'], port=settings['port'], user=usr, password=passwd, dbName=settings['dbName'])

  if settings['remainder']=="delete" or settings['remainder']=="recreate":
    print "deleting now"
#    dbConnectionService.deleteFromDB('RPCPVSSPERLUMI')
#    dbConnectionService.dropFromDB('RPCPVSSPERLUMI')
#    dbConnectionService.deleteFromDB('RPCPVSSPERRUN')
#    dbConnectionService.dropFromDB('RPCPVSSPERRUN')


    dbConnectionService.deleteFromDB('TESTOUT_PVSS')
    dbConnectionService.dropFromDB('TESTOUT_PVSS')
#    dbConnectionService.deleteFromDB('TESTOUTPVSS')
#    dbConnectionService.dropFromDB('TESTOUTPVSS')


#    dbConnectionService.deleteFromDB('RPCPVSSPERRUUN')
#    dbConnectionService.dropFromDB('RPCPVSSPERRUUN')
    sys.exit()
    #return
  
  dbServicePVSSPERLUMI = runDataToDB_PVSSPERRUN_ORACLE_OMDS.DBService(dbConnectionService.engine())
  if settings['dbType'] == "sqlite:///":
    dbServicePVSSPERLUMISQL = runDataToDB_PVSSPERRUN_SQL.DBService(dbConnectionService.engine())

#  dbServicePVSSPERLUMI = runDataToDB_PVSSPERLUMI_PVSSPERRUN_ORACLE_OMDS.DBService(dbConnectionService.engine(),schema)
  if settings['remainder']=="create" or settings['remainder']=="recreate":
    if  settings['dbType'] == "sqlite:///":
      dbServicePVSSPERLUMISQL.createDBPERRUN()
    else:
      dbServicePVSSPERLUMI.createDBPERRUN()
#  if settings['remainder']=="create":
#    dbServicePVSSPERLUMI.createDBPERLUMI()

  #  dbServicePVSSPERLUMI = runDataToDB_PVSSPERLUMI_PVSSPERRUN_ORACLE.DBService(dbConnectionService)
  #dbServicePVSSPERLUMI.createDB()
  
  #   dbServiceEFFICIENCY = runDataToDB_EFFICIENCY_ORACLE_OMDS.DBService(dbConnectionService.engine(),schema)
  #  dbServiceEFFICIENCY = runDataToDB_EFFICIENCY_ORACLE.DBService(dbConnectionService)
  #dbServiceEFFICIENCY.createDB()


#Read part Good:  
  if  settings['dbType'] == "sqlite:///":
     fileReaderPVSSPERRUN = runDataToDB_PVSSPERRUN_SQL.FileReaderPVSSPERRUN()
  else:  
    fileReaderPVSSPERRUN = runDataToDB_PVSSPERRUN_ORACLE_OMDS.FileReaderPVSSPERRUN()
####

  
  first = True
##  for run in runlistPVSSPERLUMI:
  for run in runlistDQM:

#    continue
    print run


#Read part Good
    dataPVSSPERRUN = fileReaderPVSSPERRUN.readImonVmon(directory+str(run)+"_ImonPerRun.txt",directory+str(run)+"_VmonPerRun.txt")
#############
    insertList = dbServicePVSSPERLUMI.insertToDB(dataPVSSPERRUN,None)
#    print insertList
    for ins in insertList:
#       print " ins in insertlist is : "
#       print ins
      dbConnectionService.insertToDB(ins)
      
    
    
    # -s sqliteDB.db fileName.            
    
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
