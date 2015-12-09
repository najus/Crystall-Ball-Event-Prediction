package edu.mum.crystalball.hybrid;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Text> {

	private int total;
	private Map<Pair, Integer> map;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		total = 0;
		map = new HashMap<>();
	}

	@Override
	protected void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		total = sumAllValues(values);
		System.out.println(key + " :: " + total);
		map.put(key, total);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println(map);
		Map<String, Integer> mp = new HashMap<>();

		String wPrev = "";

		for (Map.Entry<Pair, Integer> entry : map.entrySet()) {
			String w = entry.getKey().getKey().toString();

			if (!w.equals(wPrev) && mp.size() != 0) {
				total = 0;

				for (Map.Entry<String, Integer> e : mp.entrySet()) {
					total += e.getValue();
				}

				DecimalFormat decimalFormat = new DecimalFormat("0.000");
				StringBuilder stringBuilder = new StringBuilder();

				for (Map.Entry<String, Integer> e : mp.entrySet()) {
					if (stringBuilder.length() > 0)
						stringBuilder.append(",");
					stringBuilder.append("(").append(e.getKey()).append(",")
							.append(decimalFormat.format(e.getValue() / (double) total)).append(") ");
				}

				context.write(new Text(wPrev), new Text("[ " + stringBuilder.toString() + "] "));
			} else {
				String u = entry.getKey().getValue().toString();
				if (!mp.containsKey(u))
					mp.put(u, 0);
				mp.put(u, mp.get(u) + 1);
				wPrev = w;
			}
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
