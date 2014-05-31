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
tweets = db.tweets

def get_instance(conf):
    return TCNgramMapperLoader(conf)


class TCNgramMapperLoader(Plugin):

    def __init__(self, conf=None):
        self.suffix = 'gram'
        self.target = 'tweets'
        self.limit = -1
        self.set_conf(conf)
                
    def execute(self, data):
        target = self.target
        


        if self.db:
            db = con[self.db]
            collection = db[self.collection]

        if self.ngram:
            self.suffix = str(self.ngram) + 'gram.mapped'

        out = '%s.%s' %(target, self.suffix)
        data['in'] = out

        n = int(self.ngram)

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
            text = text.replace(self.mr_delimiter, ' ')

            for i in range(len(text)):
                for j in range(n):
                    ngram = text[i:i+j+1]
                    oio.write('%s%s1\n' %(ngram, self.mr_delimiter))
                    
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
