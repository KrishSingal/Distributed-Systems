#!/bin/bash
iter=1
> output
while [ "$iter" -lt 6 ]
do
  echo "$iter"
	echo "$iter" >> output
	go test -run TestConcurrent >> output
	iter=`expr $iter + 1`
done