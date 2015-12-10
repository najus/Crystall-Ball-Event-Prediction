package edu.mum.crystalball.mapreduce;

import org.apache.commons.cli.CommandLine;

/**
 * 
 * @author sachinkeshav
 *
 */
public interface JobLoader {

	public int process(CommandLine commandLine, MyOptions options)  throws Exception;
}
