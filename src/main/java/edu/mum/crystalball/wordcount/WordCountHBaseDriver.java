package edu.mum.crystalball.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.mum.crystalball.wordcount.WordCountDriver;

public class WordCountHBaseDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCountHBaseDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration conf = HBaseConfiguration.create();
//		HBaseAdmin admin = new HBaseAdmin(conf);
//
//		// creating table descriptor
//		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("wordcount"));
//
//		// creating column family descriptor
//		HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("details"));
//
//		// adding coloumn family to HTable
//		table.addFamily(family);
//
//		// creating HTable
//		admin.createTable(table);
//		admin.close();

		Job job = new Job(conf, "HBaseWordCount");
		job.setJarByClass(WordCountDriver.class);
		job.setJobName("HBaseWordCount");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("wordcount", WordCountTableReducer.class, job);
		
		job.setMapperClass(WordCountMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		return returnValue;
	}
}
