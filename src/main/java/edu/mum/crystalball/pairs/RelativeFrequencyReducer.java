package edu.mum.crystalball.pairs;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFrequencyReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	int totalCount = 0;

	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		DecimalFormat decimalFormat = new DecimalFormat("0.000");
		if (pair.getValue().toString().equals("*")) {
			totalCount = sumAllValues(values);
		} else {
			int sum = sumAllValues(values);
			String value = decimalFormat.format((double) sum / (double) totalCount);
			context.write(pair, new DoubleWritable(Double.parseDouble(value)));
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
