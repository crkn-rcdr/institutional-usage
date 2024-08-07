# This script copies log files from one platform server to my personal VM. Currently the smbclient utility is 
# not installed on the platform servers so I'm copying the files over to my VM before transferring them to the 
# usagereport share on teran.


#!/bin/bash

# Define variables
REMOTE_USER="abhinavk"
REMOTE_HOST="arinto.uab.c7a.ca"
REMOTE_PATH="/var/log/"
LOCAL_PATH="/home/abhinavk/Logs/Arinto/"

# Use scp to copy files
scp "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}haproxy-traffic*" "$LOCAL_PATH"

# Verify the operation
if [ $? -eq 0 ]; then
  echo "Files copied successfully."
else
  echo "Error occurred during file transfer."
fi
