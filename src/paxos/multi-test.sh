#!/bin/bash
iter=1
> results
while [ "$iter" -lt 11 ]
do
	# echo "$iter"
	go test > output
	echo "$iter" >> results
	tail -3 output >> results
	iter=`expr $iter + 1`
done