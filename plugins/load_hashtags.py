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
db = con.tweets2
tweets = db.tweets2

def get_instance(conf):
    return HashtagLoader(conf)


class HashtagLoader(Plugin):

    def __init__(self, conf=None):
        self.suffix = 'hashtag'
        self.target = 'twitter'
        
        self.set_conf(conf)
                
    def execute(self, data):
        target = self.target
        
        out = '%s.%s' %(target, self.suffix)

        data['in'] = out
        if self.skip:
            return data

        if self.db:
            db = con[self.db]
            collection = db[self.collection]

        oio = open(out, 'w')
        for tw in collection.find():
            tid = tw['id']

            hts = tw.get('entities').get('hashtags')
            for ht in hts:
                text = ht.get('text')
                oio.write('%d%s%s\n' %(tid, self.mr_delimiter, text))

        oio.close()
        return data

        
"""
main for test

"""

if __name__ == '__main__':
    m = get_instance({})
    m.execute({})
