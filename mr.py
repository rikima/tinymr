#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import os
import imp

from new import *

import yaml

debug = False
stderr = sys.stderr
stdout = sys.stdout

class PluginManager(object):
    """
    plugin loader class

    """
    
    def __init__(self, folder = "./plugins"):
        """
        init 

        """
        sys.path.append(folder)
        for file in os.listdir(folder):
            self.load_plugin(file)

  
    def load_plugin(self,file):
        """
        load plugin

        """
        if file.endswith('.py'):
            modname = file.split('.')[0]
            if debug:
                print modname
                
            sys.path.append(modname)




class MRManager(object):
    __conf = None


    def __init__(self, plugin_dir="./plugins"):
        p = PluginManager(folder=plugin_dir)

  
    def load_config(self,fname):
        """
        load config

        """

        c = open(fname).read()
        self.__conf = yaml.load(open(fname))
            
        if debug:
            stderr.write('self.__conf %s' %(self.__conf))

      

    def execute(self):
        """
        load modules
        
        """
        data = {}
        for c in self.__conf:
            modname = c['module']
            cf = c['config']
            
            f,n,d = imp.find_module(modname)
            plugin = imp.load_module(modname,f,n,d)

            
            po = plugin.get_instance(cf)
            data = po.execute(data)



if __name__ == '__main__':

    if len(sys.argv) < 2:
        print 'please input config yaml.'
        sys.exit(1)

        
    conf_file = sys.argv[1]

    mr = MRManager()
    mr.load_config(conf_file)
    mr.execute()
