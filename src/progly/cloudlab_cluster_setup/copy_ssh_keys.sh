echo "assumes there is a local file 'nodes.txt' with the correct node names of all machines:clientX through osdX"
echo "assumes this node has its ~/.ssh/id_rsa key present and permissions are 0600"
echo "will copy ssh keys and known host signatures to the following nodes in 10 seconds:"

FILE1="nodes.txt"
if test -f "$FILE1"; then
    echo "$FILE1 present, OK."
else
    echo "$FILE1 not present but required."
    exit 1
fi
FILE2="${HOME}/.ssh/id_rsa"
if test -f "$FILE2"; then
    echo "$FILE2 present, OK."
else
    echo "$FILE2 not present but required."
    exit 1
fi
cat nodes.txt
sleep 10

for n in `cat nodes.txt`; do
  ssh-keyscan -H $n >> ~/.ssh/known_hosts
done;

for n in `cat nodes.txt`; do
  ssh $n 'hostname'
  scp -p  ~/.ssh/id_rsa $n:~/.ssh/
  scp -p  ~/.ssh/known_hosts $n:~/.ssh/
#  ssh $n 'cat /etc/at.deny'
#  ssh $n 'sudo cat /etc/at.deny'
done;

