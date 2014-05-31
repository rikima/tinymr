#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import re

from plugin import Plugin
from mapper import Mapper

import simplejson as json

from pymongo import Connection

#コネクション作成
con = Connection('127.0.0.1', 27017)
#コネクションからtestデータベースを取得
db = con.ja
collection = db.tweets

def get_instance(conf):
    return TwitterClientLoader(conf)


class TwitterClientLoader(Plugin):

    def __init__(self, conf=None):
        self.suffix = 'clients'
        self.target = 'twitter'
        
        self.set_conf(conf)
                
    def execute(self, data):
        target = self.target
        
        out = '%s.%s' %(target, self.suffix)

        data['in'] = out
        if self.skip:
            return data
        
        db = con[self.db]
        collection = db[self.collection]

        pat = re.compile(r'>([^>]+)</')
        oio = open(out, 'w')
        for tw in collection.find():
            tid = tw['id']
            source = tw['source']
            
            m = pat.search(source)
            if m:
                client = m.group(1)
            else:
                client = source

            oio.write('%d%s%s\n' %(tid, self.mr_delimiter, client))

        oio.close()
        return data

        
"""
main for test

"""

if __name__ == '__main__':
    m = get_instance({})
    m.execute({})
