package edu.mum.crystalball.stripes;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends Reducer<Text, HashMap<Text, IntWritable>, Text, HashMap<Text, IntWritable>> {

	@Override
	protected void reduce(Text key, Iterable<HashMap<Text, IntWritable>> value, Context context)
			throws IOException, InterruptedException {

	}
}
