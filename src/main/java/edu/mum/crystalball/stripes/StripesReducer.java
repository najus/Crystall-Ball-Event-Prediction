package edu.mum.crystalball.stripes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
	MapWritable sumOfAllStripes = new MapWritable();

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
			stringBuilder.append("(").append(entry.getKey().toString()).append(",")
					.append(decimalFormat.format(Integer.parseInt(entry.getValue().toString()) / (double) totalCount))
					.append(") ");
		}

		context.write(key, new Text("[ " + stringBuilder.toString() + "] "));

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
