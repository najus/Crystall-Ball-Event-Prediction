package edu.mum.crystalball.mapreduce.wordcount;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.mum.crystalball.mapreduce.JobLoader;
import edu.mum.crystalball.mapreduce.MyOptions;
import edu.mum.crystalball.mapreduce.utils.ResourceUtils;

/**
 * 
 * @author sachinkeshav
 *
 */
public class WordCountLoader implements JobLoader {

	public int process(CommandLine commandLine, MyOptions options) throws Exception {
		
		String jobConfigFilename = commandLine.getOptionValue(options.jobConfig.getOpt());
		Properties jobConfigProperties = ResourceUtils.loadProperties(jobConfigFilename);
		
		String inputFile = jobConfigProperties.getProperty("inputFile");
		String outputDir = jobConfigProperties.getProperty("outputDirectory");
		String loaderName = jobConfigProperties.getProperty("loaderName");
		
		if  (inputFile == null) {
            System.out.println("inputFile is missing");
            return 1;
        }
        if  (outputDir == null) {
            System.out.println("outputDirectory is missing");
            return 1;
        }

		Job job = new Job();
		job.setJarByClass(WordCountLoader.class);
		job.setJobName(loaderName);

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		return returnValue;
	}
}
