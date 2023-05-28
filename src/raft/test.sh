#!/bin/bash
# for i in {0..500}
# do
#     go test -run 2 --race
# done


for i in {0..500}
do
    go test -run 2 > errTEST2.md
    if [ $? -ne 0 ];then
        echo "fail"
        break
    else
        echo "success"
    fi
done
