#!/bin/sh

HOST=192.168.1.3
USER='anonymous'
PASSWD='guest'
SRCPATH=$1
DSTPATH="/home/textd/data/itemxml/$1"
mkdir -p $DSTPATH
cd $DSTPATH

ftp -n $HOST <<EOF
user $USER $PASSWD
cd $SRCPATH
prom
mget * 
EOF

exit 0

