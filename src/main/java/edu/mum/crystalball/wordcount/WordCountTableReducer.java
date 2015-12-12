package edu.mum.crystalball.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	
	private int count = 1;

	@SuppressWarnings("deprecation")
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			sum = sum + valuesIt.next().get();
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<").append(key.toString()).append(" - ").append(sum).append(">");
		
		Put put = new Put(Bytes.toBytes("row-" + count));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("wc"), Bytes.toBytes(sb.toString()));

		context.write(new ImmutableBytesWritable(Bytes.toBytes(count)), put);
		count++;
	}
}
