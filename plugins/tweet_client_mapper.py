#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import re

from mapper import Mapper

def get_instance(conf):
    return TweetClientMapper(conf)


class TweetClientMapper(Mapper):
    def __init__(self, conf):
        self.suffix = 'mapped'
        self.skip = False
        
        self.set_conf(conf)

        self.reject_pat = re.compile(r'\d+')



    def execute(self, data):
        target = data.get('in')
        output = '%s.%s' %(target, self.suffix)
        
        data['in'] = output
        if self.skip:
            return data
        
        pat = re.compile(r'>([^<>]+)</')

        io = open(target, 'r')
        oo = open(output, 'w')
        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)

            docid = ss[0]
            tweet = ss[1]

            tw = json.loads(tweet)
            
            tid = int(tw['id'])
            source = tw['source']
            
            m = pat.search(source)
            client = m.group(1)
            
            oo.write('%s%s1\n' %(client, self.mr_delimiter))

            if self.debug:
                print '#%d' %(i)


        oo.close()
        io.close()


        return data

        
if __name__ == '__main__':
    print 'mapper.py'
    
