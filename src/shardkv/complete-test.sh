#!/bin/bash
iter=1
> results
while [ "$iter" -lt 21 ]
do
	echo "$iter"
	go test > output
	echo "$iter" >> results
	tail -3 output >> results
	tail -3 results | cat
	iter=`expr $iter + 1`
done