#!/usr/bin/env python
# encoding: utf-8
"""
XMLzipper.py

Created by Joe on 2011-09-23.
Copyright (c) 2011 CIC. All rights reserved.
"""

import sys
import os

def deleteDir(dirPath):
    for root, dirs, files in os.walk(dirPath):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(dirPath)
    print "Successful delete ", dirPath

def main():
    pass
    XMLpath = "/home/te_opr/TextEngine/Finished"
    
    #load all directories
    for parent, dirs, files in os.walk(XMLpath):
        for dirname in dirs:
        #for each directory, compress
            target = parent+os.sep+dirname+'.tar.gz'
            source = parent+os.sep+dirname
            zipCmd = "tar czf %s %s" % (target, source)
            if os.system(zipCmd) == 0:
                print "Successful backup ", target
                #delete the directory
                deleteDir(source)
            
            else :
                print "Backup failed ", target


if __name__ == '__main__':
    main()
