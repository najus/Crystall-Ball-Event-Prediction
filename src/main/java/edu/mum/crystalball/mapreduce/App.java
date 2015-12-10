package edu.mum.crystalball.mapreduce;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;

import edu.mum.crystalball.mapreduce.utils.ClassUtils;
import edu.mum.crystalball.mapreduce.utils.ResourceUtils;
import edu.mum.crystalball.mapreduce.utils.SetUtils;
import edu.mum.crystalball.mapreduce.wordcount.WordCount;

/**
 * 
 * @author sachinkeshav
 *
 */
public class App {
	
	static String packagePath = "edu.mum.crystalball.mapreduce.";
	
	static Set<String> appPropertiesSet = new HashSet<String>(SetUtils.setFromFile("loaderProperties/loaderProperties.list"));
	
	@SuppressWarnings("rawtypes")
	static Map<String, Class> jobLoaders = new HashMap<>();
	
	static {
		jobLoaders.put("WordCount", WordCount.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static JobLoader getLoader(String jobName, Map<String, Class> jobLoaders) {
        if (!jobLoaders.containsKey(jobName)) {
            throw new RuntimeException("no handler defined for the job with name: " + jobName);
        }
        return (JobLoader) ClassUtils.getInstance(jobLoaders.get(jobName));
    }
	
	@SuppressWarnings("rawtypes")
	private static JobLoader getLoaderFromProperties(String jobName){

        Properties loaderProperties;
		Class cls = null;

        if (appPropertiesSet.contains(jobName)) {
            loaderProperties = ResourceUtils.loadProperties("classPath:loaderProperties/" + jobName + ".properties");
            if (!loaderProperties.stringPropertyNames().contains(jobName)) {
                loaderProperties = ResourceUtils.loadProperties("classPath:loaderProperties/default.properties");
            }
        } else
            loaderProperties = ResourceUtils.loadProperties("classPath:loaderProperties/default.properties");

        if(!loaderProperties.stringPropertyNames().contains(jobName)) {
            throw new RuntimeException("no handler defined for the job with name: " + jobName);
        }

        try {
            cls = Class.forName(packagePath + loaderProperties.getProperty(jobName));

            System.out.println("##### Loader : " + loaderProperties.getProperty(jobName) + " #####");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return (JobLoader) ClassUtils.getInstance(cls);
    }
	
	public static void main(String[] args) throws Exception {
		
		MyOptions options = MyOptions.createOptions();
		CommandLine commandLine = options.parse(args);
		
		if(commandLine!= null){
			if(commandLine.hasOption(options.job.getOpt())) {
				String job = commandLine.getOptionValue(options.job.getOpt());
				
                if (commandLine.hasOption(options.jobConfig.getOpt())) {
                    JobLoader loader = getLoaderFromProperties(job);
                    int trapInfo = loader.process(commandLine, options);
                    System.out.println("Processing completed. Exiting with status " + trapInfo);

                } else {
                    JobLoader loader = getLoader(job, jobLoaders);
                    int trapInfo = loader.process(commandLine, options);
                    System.out.println("Processing completed. Exiting with status " + trapInfo);
                }
			}else {
				options.printHelp();
			}
		}
	}
}
