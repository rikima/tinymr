#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import re

from plugin import Plugin
from mapper import Mapper

import simplejson as json

from pymongo import Connection

from mecabparser import MecabParser

#コネクション作成
con = Connection('127.0.0.1', 27017)
#コネクションからtestデータベースを取得
db = con.ja
tweets = db.tweets

def get_instance(conf):
    return TweetsUserMapperLoader(conf)


class TweetsUserMapperLoader(Plugin):

    def __init__(self, conf=None):
        self.target = 'tweets'
        self.limit = -1
        self.suffix = 'user.mapped'
        self.set_conf(conf)

    def execute(self, data):
        target = self.target

        if self.db:
            db = con[self.db]
            collection = db[self.collection]

        out = '%s.%s' %(target, self.suffix)
        data['in'] = out

        limit = -1
        if self.limit:
            limit = self.limit


        if self.skip:
            return data

        count = 0
        oio = open(out, 'w')
        for tw in collection.find():
            tid = tw['id']
            if tw.get('_id'):
                del tw['_id']


            text = tw.get('text')
            sname = tw.get('user').get('screen_name')

            oio.write('%s%s1\n' %(sname, self.mr_delimiter))
            
            if count > 0 and count % 1000 == 0:
                print "#", count
                
                    
            count += 1
            if limit > 0 and limit < count:
                oio.close()
                return data

            
        oio.close()
        return data

        
"""
main for test

"""

if __name__ == '__main__':
    m = get_instance({})
    m.execute({})
