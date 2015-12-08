package edu.mum.pairs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {

	private Map<Pair, Integer> map = new HashMap<>();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] allTerms = value.toString().split("\\s+");

		for (int i = 0; i < allTerms.length; i++) {
			String term = allTerms[i];
			for (int j = i + 1; j < allTerms.length; j++) {
				if (term.equals(allTerms[j]))
					break;

				Pair keyTerm = new Pair(term, allTerms[j]);

				Pair starTerm = new Pair(term, "*");

				if (!map.containsKey(starTerm)) {
					map.put(starTerm, 1);
				} else {
					map.put(starTerm, map.get(starTerm) + 1);
				}

				if (!map.containsKey(keyTerm)) {
					map.put(keyTerm, 1);
				} else {
					map.put(keyTerm, map.get(keyTerm) + 1);
				}
			}
		}
		for (Map.Entry<Pair, Integer> entry : map.entrySet()) {
			context.write(entry.getKey(), new IntWritable(entry.getValue()));
		}
	}

}
