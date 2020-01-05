#!/bin/bash

set -e
disk=""

if [ -z "$1" ]
then
  echo "\$1 is empty"
  echo "please pass disk name (e.g., sdc) as \$1"
  echo "forcing program exit..."
  exit 1
else
  disk=$1 
  echo "using dev '${disk}'" 
fi

#sudo mkfs -t ext4 /dev/$disk;
sudo mkdir -p /mnt/$disk;
sudo mount /dev/$disk /mnt/$disk;
sudo chmod a+rw /mnt/$disk
df -h 
echo "done mount ${disk} at path /mnt/${disk} on hostname=" 
hostname

