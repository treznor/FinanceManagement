#!/bin/sh

find /mnt/c/users/jmfel/Downloads/*.CSV -newer 'download.tmp' -type f | while read line
do
    cp $line ./transactions
done

touch download.tmp