#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
import re
import datetime

buffer_size=0
total_count=0
splited={}

def clear_format(match_object):
  return match_object.group(1)

def flush_objects():
  for dt, objects in splited.items():
    outfile=open(dt,'a')
    outfile.write("%s\n" % "\n".join(json.dumps(o,sort_keys=True, ensure_ascii=True) for o in objects))
    outfile.close()

for read_line in sys.stdin:
  read_line = re.sub('Date\( ([^\)]+) \)', clear_format, read_line.replace("\n",""))
  read_line = re.sub('NumberLong\(([^\)]+)\)', clear_format, read_line)
  o = json.loads(read_line)
  buffer_size += 1
  total_count += 1
  dt = datetime.datetime.fromtimestamp(o['created_at']/1000).strftime('%Y-%m-%d')
  if dt not in splited:
    splited[dt] = []
  splited[dt].append(o)
  if (buffer_size == 100000):
    flush_objects()
    splited.clear()
    buffer_size = 0

flush_objects()
