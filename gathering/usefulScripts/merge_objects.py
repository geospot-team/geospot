#!/usr/bin/env python
# -*- coding: utf-8 -*-

## usage example: ./merge.py 2014-10-27 2014-11-11
## output: file 2014-10-27_2014-11-11
## files 2014-10-27.gz ... 2014-11-11.gz should be in ./

import sys
import os
import datetime
import gzip

from_date=sys.argv[1]
to_date=sys.argv[2]

def get_whole_period(from_str, to_str):
  from_dt = datetime.datetime.strptime(from_str, '%Y-%m-%d')
  to_dt = datetime.datetime.strptime(to_str, '%Y-%m-%d')
  period = []
  dt = from_dt
  while (to_dt - dt) >= datetime.timedelta(days=0):
    period.append(dt.strftime('%Y-%m-%d'))
    dt = dt + datetime.timedelta(days=1)
  return period

period = get_whole_period(from_date, to_date)
with open(from_date+'_'+to_date, 'w') as outfile:
  for dt in period:
    fname=dt+".gz"
    if os.path.isfile(fname):
      with gzip.open(fname) as infile:
        outfile.write(infile.read())


