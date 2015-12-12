package edu.mum.crystalball.pairs;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

public class PairsTableReducer extends TableReducer<Pair, IntWritable, ImmutableBytesWritable> {

	private int totalCount = 0;
	private int count = 1;

	@SuppressWarnings("deprecation")
	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		DecimalFormat decimalFormat = new DecimalFormat("0.000");
		if (pair.getValue().toString().equals("*")) {
			totalCount = sumAllValues(values);
		} else {
			int sum = sumAllValues(values);
			String value = decimalFormat.format((double) sum / (double) totalCount);
			
			StringBuilder sb = new StringBuilder();
			sb.append("<").append(pair.toString()).append(" - ").append(Double.parseDouble(value)).append(">");
			
			Put put = new Put(Bytes.toBytes("row-" + count));
			put.add(Bytes.toBytes("details"), Bytes.toBytes("rf"), Bytes.toBytes(sb.toString()));
			
			
			context.write(new ImmutableBytesWritable(Bytes.toBytes(count)), put);
			count++;
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
