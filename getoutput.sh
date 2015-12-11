#!/bin/bash

HDFS="hdfs dfs -getmerge"
OP="output"
RF="relativefrequency"

mkdir -p "$OP"/pairs "$OP"/stripes "$OP"/hybrid "$OP"/wordcount

$HDFS /wordcount/"$OP"/ "$OP"/wordcount/wc_output.txt
$HDFS /"$RF"/"$OP"/pairs/ "$OP"/pairs/pairs_output.txt
$HDFS /"$RF"/"$OP"/stripes/ "$OP"/stripes/stripes_output.txt
$HDFS /"$RF"/"$OP"/hybrid/ "$OP"/hybrid/hybrid_output.txt