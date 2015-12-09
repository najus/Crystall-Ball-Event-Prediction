package edu.mum.crystalball.stripes;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends Mapper<LongWritable, Text, Text, HashMap<LongWritable, IntWritable>> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	}
}
