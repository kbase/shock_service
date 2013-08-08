#!/bin/sh 
CMD=${1:-"install"}

if [ "$CMD" = "install" ]; then 
    if [ ! "$(ls -A ../Shock)" ]; then
        cd ..
        git submodule init
        git submodule update
        cd -
    fi
    cp ../Shock/libs/shock.js js/KBaseShock.js
    sed -e 's/^package Shock;/package Bio::Kbase::Shock;/' < ../Shock/libs/Shock.pm > perl/Bio-KBase-Shock/lib/Bio/KBase/Shock.pm
    cp ../Shock/libs/shock.py python/biokbase/shock/shock.py
elif [ "$CMD" = "clean" ]; then 
    rm js/KBaseShock.js perl/Bio-KBase-Shock/lib/Bio/KBase/Shock.pm python/biokbase/shock/shock.py
else
    echo "USAGE: prepare.sh [ clean ]"    
fi
    