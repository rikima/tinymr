#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import re

from mapper import Mapper

def get_instance(conf):
    return NgramMapper(conf)

class NgramMapper(Mapper):

    
    def __init__(self, conf, delimiter=','):
        self.delimiter = delimiter
        self.suffix = '%dgram' %(conf['ngram'])
        self.skip = False
        self.debug = False

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
        
        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)
            
            docid = ss[0]
            terms = ss[1]
            
            terms = terms.split()
            for i, t_i in enumerate(terms):
                key = t_i
                # ngram 
                for j in range(self.ngram):
                    if j > 0:
                        try:
                            key += '-' + terms[i+j]
                        except:
                            continue
                    oo.write('%s%s1\n' %(key, self.mr_delimiter))


                    

        oo.close()
        io.close()
        
        return data

        
if __name__ == '__main__':
    print 'mapper.py'
    
