#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys

from plugin import Plugin
from mapper import Mapper

from mecabparser import MecabParser

def get_instance(conf):
    return TermExtracter(conf)


class TermExtracter(Plugin):

    def __init__(self, conf=None, delimiter=","):
        self.delimiter = delimiter
        self.suffix = 'terms'
        self.debug = True
        self.skip = False
        
        self.set_conf(conf)
        
        self.splitter = MecabParser()


        
    def execute(self, data):
        #target = data['in']
        target = self.target
        
        out = '%s.%s' %(target, self.suffix)

        data['in'] = out

        if self.skip:
            return data
        
        
        io = open(target, 'r')
        oio = open(out, 'w')
        for i, l in enumerate(io):
            path = l.strip()
            
            print path

            try:
                io2 = open(path, 'r')
            except:
                continue
            
            body = io2.read()
            io2.close()

            if self.debug:
                print body

            ss = self.splitter.split(body)
            terms = ''
            for s in ss:
                for t in s:
                    terms += ' ' + t 

            terms = terms.strip()
            if len(terms) > 0:
                oio.write('%s%s%s\n' %(path, self.delimiter, terms))
            

        oio.close()
        return data

        
"""
main for test

"""

if __name__ == '__main__':
    print 'mapper.py'
    
