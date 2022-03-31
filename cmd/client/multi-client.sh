#/bin/bash

scp client yijian@10.1.6.232:~/
scp client-run.sh yijian@10.1.6.232:~/

ssh yijian@10.1.6.232 ~/client-run.sh &
./client-run.sh