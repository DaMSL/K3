#! /bin/bash

$1 -p $2 &> $4_1.txt &
$1 -p $3 &> $4_2.txt &
wait
