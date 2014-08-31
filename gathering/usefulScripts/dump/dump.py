#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import subprocess
import time
import os
import shutil

from YaDiskClient import YaDisk, YaDiskException

LOGIN="login@yandex.ru"
PSWD="pwd"

db_name=sys.argv[1]
coll_name=sys.argv[2]

if len(sys.argv) > 4:
  dest_path=sys.argv[3]
else:
  hostname = subprocess.Popen("hostname -f", shell=True,stdout=subprocess.PIPE).stdout.readline().replace('\n', '')
  dest_path="Geospot-data/gathering_dump/"+db_name+"/"+coll_name+"/"+hostname

time_prefix=str(time.time()).split(".")[0]
out_dump_dir=time_prefix+"_dump"
#use --out 
dump_cmd='mongodump --db {0} --collection {1} --out {2}'.format(db_name, coll_name, out_dump_dir)
print "running mongodump"
print dump_cmd
subprocess.check_call(dump_cmd, shell=True,stdout=subprocess.PIPE)

dump_path=os.getcwd()+'/'+out_dump_dir+'/{0}/'.format(db_name)
dump_files = [ f for f in os.listdir(dump_path) if os.path.isfile(os.path.join(dump_path,f)) ]
if len(dump_files) == 0:
  print "No files to dump for {0}.{1}".format(db_name, coll_name)
  sys.exit()
yadisk_client=YaDisk(LOGIN,PSWD)

dest_path_temp=""
for dest_path_dir in dest_path.split("/"):
  dest_path_temp += dest_path_dir+"/"
  try :
    yadisk_client.ls(dest_path_temp)
  except YaDiskException:
    yadisk_client.mkdir(dest_path_temp)

exec_dir=os.getcwd()
os.chdir(dump_path)
gzipped_dump=time_prefix+".tar.gz"
gzip_cmd='tar cfvz {1} {0} '.format(' '.join(dump_files), gzipped_dump)
print "archive dumpped files to common tar.gz"
print gzip_cmd
subprocess.check_call(gzip_cmd, shell=True,stdout=subprocess.PIPE)
print "upload to "+dest_path
yadisk_client.upload(gzipped_dump, "%s/%s" % (dest_path, time_prefix+".tar.gz"))
os.chdir(exec_dir)
shutil.rmtree(out_dump_dir)
