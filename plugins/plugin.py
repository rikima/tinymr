#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import os
import os.path
import logging

debug = False

class Plugin(object):

    def __init__(self, conf):
        self.set_conf(conf)

    def set_conf(self, conf):
        self.mr_delimiter = '\t'
        self.skip = False
        self.debug = False
        self.delete = True

        if conf:
            for k, v in conf.items():
                if debug:
                    print k, v

                setattr(self, k, v)


                
    def execute(self, data):
        self.init(data)
        self.terminate(data)
        return data

    def init(self, data):
        logging.info('init')
    
    def terminate(self, data):
        os.remove(self.target)
        
        if hasattr(self, 'output'):
            if os.path.exists(self.output):
                os.remove(self.output)
            os.rename(data['in'], self.output)

    

"""
main for test


"""
if __name__ == '__main__':
    print 'test plugin.py'

    p = Plugin()

    conf = dict(a='a', b='b', c='c')

    p.set_conf(conf)

    print dir(p)

    print p.a
    print p.b
    print p.c



