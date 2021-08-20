#!/bin/bash

File="psql.txt"
Lines=$(cat $File)
for line in $Lines
do
    echo $line
done

    

