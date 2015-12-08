package edu.mum.pairs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	int totalCount = 0;

	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		if (pair.getValue().toString().equals("*")) {
			totalCount = sumAllValues(values);
		} else {
			int sum = sumAllValues(values);
			double value = (double) sum / (double) totalCount;
			context.write(pair, new DoubleWritable(value));
		}
	}

	private int sumAllValues(Iterable<IntWritable> values) {
		int sum = 0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		return sum;
	}
}
