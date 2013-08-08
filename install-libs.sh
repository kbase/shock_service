#!/bin/sh 
TARGET=$1
LIB_DIR=$TARGET/lib
PERL=/kb/runtime/bin/perl

# prepare libs
cd libs
./prepare.sh

# install perl Bio::KBase::Shock
cd perl/Bio-KBase-Shock
${PERL} Makefile.PL
make 
make install
cd -

# install python shock
pip install python

cd ..