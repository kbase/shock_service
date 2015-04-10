#!/usr/bin/env python

import os
import sys
import argparse
import bson
import hashlib
from bson import BSON
from time import strftime

VERBOSE = False
MD5_CHECK = False

def hashfile(afile, hasher, blocksize=65536):
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()

def validate_dir_contents(ndir):
    # Each node directory should contain at least a bson file and an idx/ directory
    nid = os.path.basename(os.path.normpath(ndir))
    idir = ndir + "/idx"
    if not os.path.isdir(idir):
        sys.stdout.write("[data invalid]: node " + nid + " is missing idx directory\n")

    bfile = ndir + "/" + nid + ".bson"
    if not os.path.isfile(bfile):
        if VERBOSE:
            sys.stdout.write("[data needs repair]: node " + nid + " is missing bson file, directory should be deleted\n")
	return 0

    if os.path.getsize(bfile) == 0:
        sys.stdout.write("[data invalid]: node " + nid + " has zero size bson file\n")
	return 0

    # bson file exists and is non-zero, now validate:
    #     - Existence of any index files
    #     - Existence of data file (if node is non-empty)
    #     - Size of data file
    #     - md5 of data file (optional)

    # reading bson file
    bfh = open(bfile)
    bobj = BSON(bfh.read())
    node = bson.BSON.decode(bobj)

    # setting default data file path, if different path is set, replace this
    dfile = ndir + "/" + nid + ".data"
    if node["file"]["path"] != "":
        dfile = node["file"]["path"]

    # if the file md5 checksum is set and this is a regular node, then we know a file should exist, check for file existence, file size, and md5 (optional)
    if "md5" in node["file"]["checksum"] and node["file"]["checksum"]["md5"] != "" and ("type" not in node or node["type"] not in ["parts", "subset", "copy", "virtual"]):
        if not os.path.isfile(dfile):
            sys.stdout.write("[data invalid]: node " + nid + " is missing it's data file (path=" + dfile + ")\n")
            return 0
        dsize = os.path.getsize(dfile)
        if node["file"]["size"] != dsize:
            sys.stdout.write("[data invalid]: node " + nid + " has data file with size=" + str(dsize) + " but bson doc says file size=" + str(node["file"]["size"]) + "\n")
            return 0
        if MD5_CHECK:
            dmd5 = hashfile(open(dfile, 'rb'), hashlib.md5(), 65536)
            if dmd5 != node["file"]["checksum"]["md5"]:
                sys.stdout.write("[data invalid]: node " + nid + " has data file with md5=" + dmd5 + " but bson doc says file md5=" + node["file"]["checksum"]["md5"] + "\n")
                return 0

    return 0

def main(args):
    parser = argparse.ArgumentParser(description="Validate the contents of Shock subdirectories.")
    parser.add_argument("data_dir", metavar="DIR", help="Shock data directory")
    parser.add_argument("-m", "--md5", action="store_true", help="compute md5 on files (default: false)")
    parser.add_argument("-v", "--verbose", action="store_true", help="print warnings (default: false)")
    args = parser.parse_args()

    if not args.data_dir:
        args.data_dir = './'

    if args.md5:
        global MD5_CHECK
        MD5_CHECK = True

    if args.verbose:
        global VERBOSE
        VERBOSE = True

    count = 0
    # Traversing Shock data directories
    # Format for data directories: <data_dir>/12/34/56/123456XX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
    path1 = args.data_dir
    # path1 = <data_dir>
    for fname in os.listdir(path1):
        path2 = os.path.join(path1, fname)
        if os.path.isdir(path2):
            # path2 = <data_dir>/12
            for fname in os.listdir(path2):
                path3 = os.path.join(path2, fname)
                if os.path.isdir(path3):
                    # path3 = <data_dir>/12/34
                    for fname in os.listdir(path3):
                        path4 = os.path.join(path3, fname)
                        if os.path.isdir(path4):
                            # path4 = <data_dir>/12/34/56
                            for fname in os.listdir(path4):
                                path5 = os.path.join(path4, fname)
                                if os.path.isdir(path5):
                                    # path5 = <data_dir>/12/34/56/123456XX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
                                    validate_dir_contents(path5)
                                    count += 1
                                    if count%1000 == 0:
                                        print strftime("%Y-%m-%d %H:%M:%S") + " " + str(count)

    return 0

if __name__ == "__main__":
    sys.exit( main(sys.argv) )
