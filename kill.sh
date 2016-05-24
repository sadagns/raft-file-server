#!/bin/bash

proc=$1
a=$(ps -aux | grep -v "grep" | grep -v "kill" | grep -w -e $proc | awk '{print $2}')
for i in $a
do
	sudo kill -9 $i
done	
