package edu.mum.crystalball.mapreduce;

import org.apache.commons.cli.*;

/**
 * 
 * @author sachinkeshav
 *
 */
public class MyOptions {

    public Option job;
    public Option jobConfig;
    protected Options options;

    protected MyOptions() {
    }

    public static MyOptions createOptions() {
        MyOptions myOptions = new MyOptions();
        myOptions.initializeOptions();
        return (myOptions);
    }

	public CommandLine parse(String[] args) {
        try {
            GnuParser parser = new GnuParser();
            CommandLine commandLine = parser.parse(options, args);
            return (commandLine);
        } catch (ParseException pe) {
            System.out.println(pe);
            return (null);
        }
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop jar MapReduce.jar", options);
    }

    protected void initializeOptions() {
        job         = addOptionWithArg("job", "Job to run");
        jobConfig   = addOptionWithArg("jobConfig", "Job Config file");
        options = new Options();
        options.addOption(job);
        options.addOption(jobConfig);
    }

	private Option addOptionWithArg(String option, String description) {
        OptionBuilder.withArgName(option);
        OptionBuilder.withDescription(description);
        OptionBuilder.hasArg();
        Option argOption = OptionBuilder.create(option);
        return (argOption);
    }
}
