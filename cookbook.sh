#!/bin/bash
# Description: Cookbook to run hadoop jobs
# Author: sachinkeshav
# Date: December 9, 2015
# Note: Script not complete, still in development. :)

HADOOP="hadoop"

JAR="target/crystalball.jar"

$HADOOP jar $JAR -job WordCount -jobConfig classPath:jobconfig/wordcount/WordCount.jobcfg