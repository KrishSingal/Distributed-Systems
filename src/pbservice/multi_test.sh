#!/bin/bash
iter=1
while [ "$iter" -lt 10 ]
do 
	echo "$iter"
	go test
	iter=`expr $iter + 1`
done
