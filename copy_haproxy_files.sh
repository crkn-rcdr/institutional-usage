# This script copies log files from all platform servers to the sambashare. Currently the smbclient utility is 
# not installed on the platform servers so I'm copying the files over to my VM before transferring them to the 
# appropriate folders in the usagereport share on teran.

# Runtime: 4 minutes

#!/bin/bash

# Define variables
REMOTE_USER="abhinavk"
REMOTE_HOSTS=("arinto.uab.c7a.ca" "altano.uab.c7a.ca" "tokaji.tor.c7a.ca" "traminac.tor.c7a.ca")
REMOTE_PATH="/var/log/"
LOCAL_PATHS=("/home/abhinavk/Logs/Arinto/" "/home/abhinavk/Logs/Altano/" "/home/abhinavk/Logs/Tokaji/" "/home/abhinavk/Logs/Traminac/")
SAMBA_SHARE="teran.tor.c7a.ca/usagereport"
SAMBA_PATHS=("Logs/Arinto" "Logs/Altano" "Logs/Tokaji" "Logs/Traminac")
SAMBA_USER="abhinavk"

# Loop through each remote host and corresponding local path
for i in "${!REMOTE_HOSTS[@]}"; do
  REMOTE_HOST="${REMOTE_HOSTS[$i]}"
  LOCAL_PATH="${LOCAL_PATHS[$i]}"
  SAMBA_PATH="${SAMBA_PATHS[$i]}"

  # Use scp to copy files to local path
  scp "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}haproxy-traffic*" "$LOCAL_PATH"

  # Verify the operation
  if [ $? -eq 0 ]; then
    echo "Files copied successfully from ${REMOTE_HOST} to ${LOCAL_PATH}."

    # Use smbclient to copy files to Samba share
    smbclient "//${SAMBA_SHARE}" -U "${SAMBA_USER}%${SAMBA_PASS}" -c "prompt; lcd ${LOCAL_PATH}; cd ${SAMBA_PATH}; mput haproxy-traffic*"

    # Verify the operation
    if [ $? -eq 0 ]; then
      echo "Files copied successfully to Samba share ${SAMBA_PATH}."
    else
      echo "Error occurred during file transfer to Samba share ${SAMBA_PATH}."
    fi
  else
    echo "Error occurred during file transfer from ${REMOTE_HOST}."
  fi
done

