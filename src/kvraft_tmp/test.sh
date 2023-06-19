#!/bin/bash
for i in {0..500}
do
    go test -run TestSnapshotRecover3B > logerr.md
    if [ $? -ne 0 ];then
        echo "fail"
        break
    else
        echo "success"
    fi
done
