#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys

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
        
        if self.db:
            db = con[self.db]
            col = db[self.collection]

        oio = open(out, 'w')
        for tw in col.find():
            if tw.get('_id'):
                del tw['_id']

            tid = tw['id']
            jsondata = json.dumps(tw)
            oio.write('%d%s%s\n' %(tid, self.mr_delimiter, jsondata))

        oio.close()
        return data

        
"""
main for test

"""

if __name__ == '__main__':
    m = get_instance({})
    m.execute({})
