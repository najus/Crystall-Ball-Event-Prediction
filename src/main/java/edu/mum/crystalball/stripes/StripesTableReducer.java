package edu.mum.crystalball.stripes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StripesTableReducer extends TableReducer<Text, MapWritable, ImmutableBytesWritable> {
	MapWritable sumOfAllStripes = new MapWritable();
	private int count = 1;

	@Override
	protected void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		sumOfAllStripes.clear();

		for (MapWritable value : values) {
			addStripeToFinalStripe(value);
		}

		int totalCount = 0;

		for (Entry<Writable, Writable> entry : sumOfAllStripes.entrySet()) {
			totalCount += ((IntWritable) entry.getValue()).get();
		}
		DecimalFormat decimalFormat = new DecimalFormat("0.000");
		StringBuilder stringBuilder = new StringBuilder();

		for (Entry<Writable, Writable> entry : sumOfAllStripes.entrySet()) {
			if (stringBuilder.length() > 0)
				stringBuilder.append(",");
			stringBuilder.append(" (").append(entry.getKey().toString()).append(",")
					.append(decimalFormat.format(Integer.parseInt(entry.getValue().toString()) / (double) totalCount))
					.append(")");
		}

		Put put = getPut(key.toString(), stringBuilder);
		
		context.write(new ImmutableBytesWritable(Bytes.toBytes(count)), put);
		count++;
	}
	
	@SuppressWarnings("deprecation")
	private Put getPut(String key, StringBuilder stringBuilder) {
		StringBuilder sb = new StringBuilder();
		sb.append("<").append(key).append(" - ").append("[" + stringBuilder.toString() + " ]").append(">");
		
		Put put = new Put(Bytes.toBytes("row-" + count));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("rf"), Bytes.toBytes(sb.toString()));
		return put;
	}

	private void addStripeToFinalStripe(MapWritable value) {

		Set<Writable> allKeys = value.keySet();
		for (Writable key : allKeys) {
			IntWritable keyCountInCurrentStripe = (IntWritable) value.get(key);
			if (sumOfAllStripes.containsKey(key)) {
				IntWritable keyCountInResultantStripe = (IntWritable) sumOfAllStripes.get(key);
				keyCountInResultantStripe.set(keyCountInCurrentStripe.get() + keyCountInResultantStripe.get());
			} else {
				sumOfAllStripes.put(key, keyCountInCurrentStripe);
			}
		}

	}

}
