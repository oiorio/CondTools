#!/usr/bin/python
import os
import re
import sys
import commands
import xmlrpclib

import json ,smtplib,email,sys,getopt
#from rrapi import RRApi, RRApiError

from runs_dqm_ran import runs_in_db

dir = "CondTools/RPC/bin/"

goodruns =[]

for run in runs_in_db:
    runstring = str(run)
    vmon_stdin,vmon_stdout,vmon_stderr= os.popen3("ls "+dir+ " | grep Vmon | grep "+runstring)
#    print  " did I find Vmon for run " +runstring+" "+str(  vmon_stdout.read())

    imon_stdin,imon_stdout,imon_stderr= os.popen3("ls "+dir+ " | grep Imon | grep "+runstring)
#    print  " did I find Imon for run " +runstring+" "+str(  imon_stdout.read())
    
    if (not str(vmon_stdout.read())== ""):
        if (not str(imon_stdout.read())== ""):
            goodruns.append(run)
            print run

fileRan = open ("runs_db_ran_total.py","w")

line = "runs_in_db =[]\n" 
fileRan.write(line)
for g in goodruns:
    fileRan.write("runs_in_db.append("+str(g)+")\n")
fileRan.close()    

fileToRun = open ("runs_dqm_to_run_total.py","w")

line = "runs_to_run_db =[]\n" 
fileToRun.write(line)
for run in runs_in_db:
    if ( not (run in goodruns) ): 
        fileToRun.write("runs_to_run_db.append("+str(run)+")\n")
fileToRun.close()    
