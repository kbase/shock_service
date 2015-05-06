#!/usr/bin/env python

import os
import sys
import argparse
import bson
import hashlib
from bson import BSON
from time import strftime
from configobj import ConfigObj
from pymongo import MongoClient

def hashfile(afile, hasher, blocksize=65536):
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()

def validate_dir_contents(ndir, node, md5):
    # Each node directory should contain at least a bson file and an idx/ directory
    nid = os.path.basename(os.path.normpath(ndir))
    idir = ndir + "/idx"
    err = 0
    if not os.path.isdir(idir):
        sys.stderr.write("[data invalid]: node " + nid + " is missing idx directory\n")
	err = 1

    bfile = ndir + "/" + nid + ".bson"
    if not os.path.isfile(bfile):
        sys.stderr.write("[data invalid]: node " + nid + " is missing bson file\n")
	err = 1
    elif os.path.getsize(bfile) == 0:
        sys.stderr.write("[data invalid]: node " + nid + " has zero size bson file\n")
	err = 1

    # setting default data file path, if different path is set, replace this
    dfile = ndir + "/" + nid + ".data"
    if node["file"]["path"] != "":
        dfile = node["file"]["path"]

    # if the file md5 checksum is set and this is a regular node, then we know a file should exist, check for file existence, file size, and md5 (optional)
    if "md5" in node["file"]["checksum"] and node["file"]["checksum"]["md5"] != "" and ("type" not in node or node["type"] not in ["parts", "subset", "copy", "virtual"]):
        if not os.path.isfile(dfile):
            sys.stderr.write("[data invalid]: node " + nid + " is missing it's data file (path=" + dfile + ")\n")
            return 1
        dsize = os.path.getsize(dfile)
        if node["file"]["size"] != dsize:
            sys.stderr.write("[data invalid]: node " + nid + " has data file with size=" + str(dsize) + " but bson doc says file size=" + str(node["file"]["size"]) + "\n")
            return 1
        if md5:
            dmd5 = hashfile(open(dfile, 'rb'), hashlib.md5(), 65536)
            if dmd5 != node["file"]["checksum"]["md5"]:
                sys.stderr.write("[data invalid]: node " + nid + " has data file with md5=" + dmd5 + " but bson doc says file md5=" + node["file"]["checksum"]["md5"] + "\n")
                return 1

    if err == 1:
        return 1

    return 0

def main(args):
    parser = argparse.ArgumentParser(description="Validate Shock nodes in mongo with their on disk files and directories. Note that command line options will override the options in the config file.")
    parser.add_argument("-c", "--config", metavar="CONFIG", type=str, help="Path to config file")
    parser.add_argument("-s", "--last_sync", metavar="DATE", type=str, help="Last sync date - datetime point from which to begin validating nodes (e.g. 2006-01-02T15:04:05-07:00). Default: check all nodes")
    parser.add_argument("-n", "--node_dir", metavar="DIR", type=str, help="Shock node data directory. Default: ./")
    parser.add_argument("-l", "--log_path", metavar="FILE", type=str, help="Path to logged output. Default: ./validate_shock_nodes.log")
    parser.add_argument("-m", "--md5", action="store_true", help="Compute md5 on files. Default: False")
    parser.add_argument("-d", "--database", metavar="DB", type=str, help="MongoDB database. Default: ShockDB")
    parser.add_argument("-o", "--db_host", metavar="HOST", type=str, help="MongoDB host/port. Default: 127.0.0.1:27017")
    parser.add_argument("-u", "--username", metavar="USER", type=str, help="MongoDB username")
    parser.add_argument("-p", "--password", metavar="PASS", type=str, help="MongoDB password")
    args = parser.parse_args()

    # Create opts dictionary with default values so we can have config options in ini file or on command line.
    # Note: options specified on the command line will over-ride those in config file, so default values are set here.
    opts = {}
    opts['md5'] = False
    opts['last_sync'] = ''
    opts['log_path'] = './validate_shock_nodes.log'
    opts['node_dir'] = './'
    opts['database'] = 'ShockDB'
    opts['host'] = '127.0.0.1:27017'
    opts['username'] = ''
    opts['password'] = ''

    if args.config:
        if not os.path.isfile(args.config):
            print "[error]: config file does not exist at location = " + args.config
            return 1
        conf=ConfigObj(args.config)
        if 'main' in conf:
            main_section = conf['main']
            if 'md5' in main_section:
                opts['md5'] = main_section['md5']
            if 'last_sync' in main_section:
                opts['last_sync'] = main_section['last_sync']
            if 'log_path' in main_section:
                opts['log_path'] = main_section['log_path']
            if 'node_dir' in main_section:
                opts['node_dir'] = main_section['node_dir']
        if 'mongo' in conf:
            mongo_section = conf['mongo']
            if 'database' in mongo_section:
                opts['database'] = mongo_section['database']
            if 'host' in mongo_section:
                opts['host'] = mongo_section['host']
            if 'username' in mongo_section:
                opts['username'] = mongo_section['username']
            if 'password' in mongo_section:
                opts['password'] = mongo_section['password']

    for key, val in vars(args).iteritems():
        if key in opts and val is not None:
            opts[key] = val

    print "\nRunning validation with the following config parameters:"
    for key, val in opts.iteritems():
        print key + " =",
	print val
    print ""

    # Error handling for parameters
    if opts['username'] == '':
        print "[INFO] username is empty, will connect to MongoDB without authentication."
    if opts['password'] == '':
        print "[INFO] password is empty, will connect to MongoDB without authentication."

    # Connect to MongoDB and iterate over nodes
    c = MongoClient(opts['host'])
    db = c[opts['database']]
    if opts['username'] != '' and opts['password'] != '':
        db.authenticate(opts['username'], opts['password'])

    nodes_passed_validation = 0
    nodes_failed_validation = 0
    for node in db.Nodes.find({}):
        nid = node['id']
        path = os.path.join(opts['node_dir'], nid[:2] + '/' + nid[2:4] + '/' + nid[4:6] + '/' + nid)
	if validate_dir_contents(path, node, opts['md5']) == 0:
            nodes_passed_validation += 1
        else:
            nodes_failed_validation += 1

    sys.stdout.write("Number of nodes that passed validation = %d\n" % nodes_passed_validation)
    sys.stdout.write("Number of nodes that failed validation = %d\n" % nodes_failed_validation)

    return 0

if __name__ == "__main__":
    sys.exit( main(sys.argv) )
