import os
import optparse


os.system("cp /nfshome0/pugliese/oiorio/newTables/runs_dqm.py .")

inputFilesDir = "/nfshome0/pugliese/oiorio/newTables/"

runlistDQM = "runs_dqm"
runlistDQMinDB = "runs_database_dqm"


#oDQMDB = open(runlistDQMinDB+".py","a")
#
#os.system("cp runs_database_blank.py listTmpDQM.py")
#
#oTMPDQM = open("listTmpDQM.py","a")
#
#runDQM = __import__(runlistDQM)
#runsDQM = runDQM.runs_in_db
#
#runDQMinDB = __import__(runlistDQMinDB)
#runsDQMinDB = runDQMinDB.runs_in_db
#
#
#directory = "/nfshome0/pugliese/oiorio/newTables/"
#
#m=False
#n=0
#nMax = 10000
#nMax = 1
#for run in runsDQM:
#  #print (run in runsDQMinDB)
#  if not run in runsDQMinDB:
#    n = n+1 
#    if n > nMax: break
##    if (int(run) >int(203746)):continue
#    print n
#    print run
#    runstring = str(run) 
#    m = True
#    #    print run
#    oTMPDQM.write("runs_in_db.append("+runstring+")\n")
#    oDQMDB.write("runs_in_db.append(" + runstring +")\n")
#
#oTMPDQM.close()
#oDQMDB.close()

m=True
if m == True :
    #os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile.py -s sqliteDB.db .")
#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSS.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p I7yeDAMPk0G83vrE delete")
#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSS.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p I7yeDAMPk0G83vrE create")
#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSS.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p I7yeDAMPk0G83vrE .")

#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSS.py -s sqlite.db create")
    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSSTEMP_2.py -s Sqlitetemp.db create")
#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSSTEMP_2.py -s Sqlitetemp.db .")
#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSSTEMP_2.py -s Sqlitetemp.db delete")

#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSS.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p I7yeDAMPk0G83vrE recreate")


#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSS.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p j6XFEznqH9f92WUf .")

#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSSTEMP.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p j6XFEznqH9f92WUf delete")
#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSSTEMP.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p j6XFEznqH9f92WUf create")
#    os.system("python runDataToDB_loop_ORACLE_OMDS_SingleFile_PVSSTEMP.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p j6XFEznqH9f92WUf .")



#os.system("python runDataToDB_loop_ORACLE_OMDS.py -o cms_orcon_prod -u Usr -p Passwd .")
