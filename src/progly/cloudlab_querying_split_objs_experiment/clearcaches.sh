echo "Usage: ./this <nosds>"
nosds=$1

for ((j=0; j<${nosds}; j++)); do
    echo "clearing cache on osd"$j
    ssh osd${j} sync
    ssh osd${j} "echo 1 | sudo tee /proc/sys/vm/drop_caches"
    ssh osd${j} sync
done
