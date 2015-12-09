package edu.mum.crystalball.hybrid;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Text> {

	private int total;
	private Map<Pair, Integer> map;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		total = 0;
		map = new TreeMap<Pair, Integer>();
	}

	@Override
	protected void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		total = sumAllValues(values);
		
		String kS = key.getKey().toString();
		String vS = key.getValue().toString();
		Pair p = new Pair(kS, vS);
		
		map.put(p, total);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Map<String, Integer> mp = new HashMap<>();

		String wPrev = "";
		total = 0;

		for (Map.Entry<Pair, Integer> entry : map.entrySet()) {
			String w = entry.getKey().getKey().toString();

			if (!w.equals(wPrev) && mp.size() != 0) {

				StringBuilder stringBuilder = getStripe(mp);
				context.write(new Text(wPrev), new Text("[" + stringBuilder.toString() + " ]"));
				
				mp.clear();
				total = 0;
				
				accumulate(mp, entry);
				wPrev = w;
			} else {
				accumulate(mp, entry);
				wPrev = w;
			}
		}
		
		StringBuilder stringBuilder = getStripe(mp);
		context.write(new Text(wPrev), new Text("[" + stringBuilder.toString() + " ]"));
	}

	private void accumulate(Map<String, Integer> mp, Map.Entry<Pair, Integer> entry) {
		String u = entry.getKey().getValue().toString();
		mp.put(u, entry.getValue());
		total += entry.getValue();
	}

	private StringBuilder getStripe(Map<String, Integer> mp) {
		DecimalFormat decimalFormat = new DecimalFormat("0.000");
		StringBuilder stringBuilder = new StringBuilder();

		for (Map.Entry<String, Integer> e : mp.entrySet()) {
			if (stringBuilder.length() > 0)
				stringBuilder.append(",");
			stringBuilder.append(" (").append(e.getKey()).append(",")
					.append(decimalFormat.format(e.getValue() / (double) total)).append(")");
		}
		return stringBuilder;
	}

	private int sumAllValues(Iterable<IntWritable> values) {
		int sum = 0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		return sum;
	}
}
