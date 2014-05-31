#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import os

from datetime import datetime
from datetime import time

from plugin import Plugin
from mapper import Mapper

import simplejson as json


from pymongo import Connection


#コネクション作成
con = Connection('127.0.0.1', 27017)
#コネクションからtestデータベースを取得
db = con.tweets
tweets = db.tweets

def get_instance(conf):
    return TweetsLoader(conf)


class TweetsLoader(Plugin):

    def __init__(self, conf=None):
        self.suffix = 'tweets'
        self.target = 'twitter'
        self.debug = False
        self.skip = False
        
        self.set_conf(conf)
                

    def execute(self, data):
        target = self.target
        
        out = '%s.%s' %(target, self.suffix)

        data['in'] = out
        if self.skip:
            return data
        
        dprefix = '.'
        if self.target_dir:
            dprefix = self.target_dir
        
        if self.db:
            db = con[self.db]
            col = db[self.collection]

        io = None
        prev_fname = None
        for i, tw in enumerate(col.find()):
            created_at = tw.get('created_at')
            
            dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
            print '#', i, created_at, dt
            
            dname = '%s/%d/%02d/%02d' %(dprefix, dt.year, dt.month, dt.day)
            if not os.path.exists(dname):
                os.makedirs(dname)
            fname = '%s/%02d.tweets.json' %(dname, dt.hour)
            
            if not prev_fname or fname != prev_fname:
                io = open(fname, 'w') 
            
            io.write('%s\f' %(str(tw)))
            
            prev_fname = fname

        return data

        
"""
main for test

"""

if __name__ == '__main__':
    m = get_instance({})
    m.execute({})
