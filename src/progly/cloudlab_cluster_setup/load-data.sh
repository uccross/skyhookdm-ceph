
format=$1
pool=$2
objfile=$3
nosds=$4
nobjs=$5

rados rmpool $pool $pool --yes-i-really-really-mean-it
rados mkpool $pool
groupsize=$((20*$nosds))
echo $groupsize
groupsize=$(( groupsize < nobjs ? groupsize : nobjs ))
for ((i=0; i < $nobjs; i+=groupsize)); do
    for ((j=0; j < groupsize; j++)); do
        oid=$(($i+$j))
        if [ "${oid}" -ge "${nobjs}" ]
        then
            break
        fi
        objname="obj.${oid}"
        echo "writing $objfile into $pool/$objname"  &
        rados -p $pool put $objname $objfile  &
    done
    wait
done
rados -p $pool df

