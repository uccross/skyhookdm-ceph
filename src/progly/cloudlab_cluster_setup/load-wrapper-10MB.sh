#!/bin/bash
set -e

cd $HOME
echo "Assumes data files are available at ${HOME}"
echo "Usage:  ./this <numosds> <numobjs>"
sleep 2s
numosds=$1
numobjs=$2
pool_ncols100_10MB_fbx="ncols100.10MB.fbx"
pool_ncols100_10MB_arrow="ncols100.10MB.arrow"
pool_lineitem_10MB_fbx="lineitem.10MB.fbx"
pool_lineitem_10MB_arrow="lineitem.10MB.arrow"
echo `date`
bash load-data.sh $pool_ncols100_10MB_fbx     fbx.ncols100.10MB.25Krows.obj.0  $numosds $numobjs;
bash load-data.sh  $pool_ncols100_10MB_arrow arrow.ncols100.10MB   $numosds $numobjs;
bash load-data.sh  $pool_lineitem_10MB_fbx     fbx.lineitem.10MB.75Krows.obj.0   $numosds $numobjs;
bash load-data.sh  $pool_lineitem_10MB_arrow  arrow.lineitem.10MB.75Krows.obj.0  $numosds $numobjs;
echo "loaded 10MB data into pools:"
echo  $pool_ncols100_10MB_fbx
echo  $pool_ncols100_10MB_arrow
echo  $pool_lineitem_10MB_fbx
echo $pool_lineitem_10MB_arrow
echo `date`





