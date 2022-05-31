#!/bin/bash

# This script does personal backups to a rsync backup server.

# directory to backup
BDIR=$1
echo $BDIR

# remote directory for backup
BACKUPDIR=/home/zhenghang/work_dir/emp-tool-rdma

# the name of the backup machine
BSERVER=CUHK_GPU01
USER=zhenghang

# your password on the backup server
# export RSYNC_PASSWORD=XXXXXX

# your ssh key
KEY=$HOME/.ssh/id_rsa


########################################################################

OPTS="-avz"

export PATH=$PATH:/bin:/usr/bin:/usr/local/bin

# conduct the actual transfer
rsync $OPTS -e "ssh -i $KEY" --exclude "build" --exclude ".git" $BDIR $USER@$BSERVER:$BACKUPDIR

# sync with the second
BSERVER=CUHK_GPU02
rsync $OPTS -e "ssh -i $KEY" --exclude "build" --exclude ".git" $BDIR $USER@$BSERVER:$BACKUPDIR

# rsync -chavzP --stats $USER@BSERVER:$BACKUPDIR/stat ../../paper/stat