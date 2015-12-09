package edu.mum.crystalball.stripes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	
	private Map<Text, MapWritable> map = new HashMap<>();
	private static final IntWritable ZERO = new IntWritable(0);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] allTerms = value.toString().split("\\s+");
		
		for (int i = 0; i < allTerms.length; i++) {
			Text term = new Text(allTerms[i]);
			
			if(!map.containsKey(term))
				map.put(term, new MapWritable());
				
			MapWritable mp = map.get(term);
			
			for (int j = i + 1; j < allTerms.length; j++) {
				if (term.equals(allTerms[j]))
					break;
				
				Text u = new Text(allTerms[j]);
				
				if(!mp.containsKey(u))
					mp.put(u, ZERO);
				
				IntWritable val = new IntWritable(Integer.parseInt(mp.get(u).toString()) + 1);
				
				mp.put(u, val);
			}
		}
		
		for(Map.Entry<Text, MapWritable> entry : map.entrySet()) {
			context.write(entry.getKey(), entry.getValue());
		}
	}
}
