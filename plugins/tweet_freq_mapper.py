#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import re

from mapper import Mapper

def get_instance(conf):
    return TweetFreqMapper(conf)

class TweetFreqMapper(Mapper):

    
    def __init__(self, conf, delimiter=','):
        self.delimiter = delimiter
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


        io = open(target, 'r')
        oo = open(output, 'w')
        
        buf = set()

        zc = 0
        
        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)


            docid = ss[0]
            terms = ss[1]

            if len(terms.strip()) == 0:
                zc += 1
                print 'zc', zc
                continue
            
            buf.clear()
            for t in terms.split():
                if self.reject_pat.match(t):
                    continue

                buf.add(t)



            for t in buf:
                oo.write('%s%s1\n' %(t, self.mr_delimiter))


            if self.debug:
                print '#%d' %(i)

        if self.debug:
            print '#zero term doc', zc

        oo.close()
        io.close()


        return data

        
if __name__ == '__main__':
    print 'mapper.py'
    
