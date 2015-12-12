#!/bin/bash
# Description: Recipe to run hadoop jobs
# Author: Sachin Kushwaha
# Date: December 10, 2015
# Note: Script not complete, still in development. :)

HDFS="hdfs"

echo "-------------------------------------------------------------------------"
echo "Removing directories if already exist."
echo "-------------------------------------------------------------------------"
$HDFS dfs -rm -r /wordcount
$HDFS dfs -rm -r /relativefrequency

echo "-------------------------------------------------------------------------"
echo "Creating the directories for wordcount and relative frequency problems."
echo "-------------------------------------------------------------------------"
$HDFS dfs -mkdir -p /wordcount/input /relativefrequency/input

echo "-------------------------------------------------------------------------"
echo "Transfering wordcount data to hdfs."
echo "-------------------------------------------------------------------------"
$HDFS dfs -put data/wordcount.txt /wordcount/input/

echo "-------------------------------------------------------------------------"
echo "Transfering relative frequency data to hdfs."
echo "-------------------------------------------------------------------------"
$HDFS dfs -put data/sample.txt /relativefrequency/input/

echo "-------------------------------------------------------------------------"
echo "Loading of data in hdfs completed."
echo "-------------------------------------------------------------------------"