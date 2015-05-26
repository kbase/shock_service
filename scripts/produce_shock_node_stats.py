#!/usr/bin/env python

import argparse
import leveldb
import os
import shutil
import sys
from configobj import ConfigObj
from pymongo import MongoClient
from uuid import UUID

def increment_leveldb_value(dbh, key):
    try:
        val = int(dbh.Get(key))
    except KeyError:
        val = None

    if val:
        dbh.Put(key, str(val+1))
    else:
        dbh.Put(key, "1")

def print_freq_dist(dbh, output_fname):
    ofh = open(output_fname, 'w')
    for i in dbh.RangeIter():
        ofh.write(i[0]+"\t"+i[1]+"\n")
    return 0

def validate_uuid(uuid):
    try:
        val = UUID(uuid, version=4)
    except ValueError:
        return False
    return str(val) == uuid

def main(args):
    parser = argparse.ArgumentParser(description="Produce some statistics about the nodes in a Shock mongo database.")
    parser.add_argument("-c", "--config", metavar="CONFIG", type=str, help="Path to config file")
    parser.add_argument("-n", "--node_dir", metavar="DIR", type=str, help="Shock node data directory. Default: ./")
    parser.add_argument("-l", "--log_path", metavar="FILE", type=str, help="Path to logged output. Default: ./validate_shock_nodes.log")
    parser.add_argument("-d", "--database", metavar="DB", type=str, help="MongoDB database. Default: ShockDB")
    parser.add_argument("-o", "--db_host", metavar="HOST", type=str, help="MongoDB host/port. Default: 127.0.0.1:27017")
    parser.add_argument("-u", "--username", metavar="USER", type=str, help="MongoDB username")
    parser.add_argument("-p", "--password", metavar="PASS", type=str, help="MongoDB password")
    args = parser.parse_args()

    # Create opts dictionary with default values so we can have config options in ini file or on command line.
    # Note: options specified on the command line will over-ride those in config file, so default values are set here.
    opts = {}
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

    print "\nRunning with the following config parameters:"
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

    dbs = {}
    db_names = ['created_on', 'last_modified', 'owner', 'filename', 'filesuffix', 'filesize']
    for name in db_names:
        db_dir = './db_'+name
        if os.path.isdir(db_dir):
            shutil.rmtree(db_dir)
        dbs[name] = leveldb.LevelDB(db_dir)

    users = {}
    for user in db.Users.find({}):
        users[user['uuid']] = user['username']

    count = 0
    for node in db.Nodes.find({}):
        created_on = str(node['created_on'])[0:7]
        last_modified = str(node['last_modified'])[0:7]
	if node['acl']['owner'] == "public":
            owner = "public"
        else:
            try:
                owner = users[node['acl']['owner']]
            except KeyError:
                owner = ""
        if validate_uuid(node['file']['name']):
            fname = "FILENAME_IS_UUID"
        else:
            fname = node['file']['name']
        fbase, fext = os.path.splitext(fname)
        fsize = str(node['file']['size'])

        # Provide db handle and key to increment the value
        increment_leveldb_value(dbs['created_on'], created_on)
        increment_leveldb_value(dbs['last_modified'], last_modified)
        increment_leveldb_value(dbs['owner'], owner)
        increment_leveldb_value(dbs['filename'], fname)
        increment_leveldb_value(dbs['filesuffix'], fext)
        increment_leveldb_value(dbs['filesize'], fsize)

        count += 1
        if count%1000 == 0:
            print count

    print_freq_dist(dbs['created_on'], "freq_dist.created_on.txt")
    print_freq_dist(dbs['last_modified'], "freq_dist.last_modified.txt")
    print_freq_dist(dbs['owner'], "freq_dist.owner.txt")
    print_freq_dist(dbs['filename'], "freq_dist.filename.txt")
    print_freq_dist(dbs['filesuffix'], "freq_dist.filesuffix.txt")
    print_freq_dist(dbs['filesize'], "freq_dist.filesize.txt")

    return 0

if __name__ == "__main__":
    sys.exit( main(sys.argv) )
