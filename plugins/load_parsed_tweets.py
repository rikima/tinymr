#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys

from plugin import Plugin
from mapper import Mapper

from mecabparser import MecabParser

import simplejson as json

from pymongo import Connection

#コネクション作成
con = Connection('127.0.0.1', 27017)
#コネクションからtestデータベースを取得
db = con.tweets
tweets = db.tweets
parsed = db.parsed

def get_instance(conf):
    return ParsedTweetLoader(conf)


class ParsedTweetLoader(Plugin):

    def __init__(self, conf=None, delimiter=","):
        self.delimiter = delimiter
        self.suffix = 'terms'
        self.debug = True
        self.skip = False
        
        self.set_conf(conf)
                
    def execute(self, data):

        target = self.target
        
        out = '%s.%s' %(target, self.suffix)

        data['in'] = out
        if self.skip:
            return data

        oio = open(out, 'w')
        for parsed_tweets in parsed.find():
            tweet_id = parsed_tweets['id']
            morphs = parsed_tweets['morphs']
            
            terms = {}
            for m in morphs:
                surface = m[0].strip()
                clazz = m[1]
            
                if len(surface) == 0:
                    continue

                if surface.find('BOS/EOS') >= 0:
                    continue
                if clazz.find(',数,') > 0:
                    continue

                if self.debug:
                    print surface, clazz

                terms[surface] = clazz
        
            termstr = ''
            for t in terms.items():
                if self.debug:
                    print t

                s = t[0]
                cs = t[1].split(',')
                termstr += '%s(%s) ' %(s, '-'.join(cs[:3])) 



            if len(terms) > 0:
                oio.write('%s%s%s\n' %(tweet_id, self.delimiter, termstr))
            

        oio.close()
        return data

        
"""
main for test

"""

if __name__ == '__main__':
    print 'mapper.py'
    execute()
