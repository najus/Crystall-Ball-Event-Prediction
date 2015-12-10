package edu.mum.crystalball.mapreduce.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * 
 * @author sachinkeshav
 *
 */
public class SetUtils {

	public static Set<String> setFromFile(String file)

	{
		HashSet<String> set = new HashSet<String>();

		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new InputStreamReader(SetUtils.class.getClassLoader().getResourceAsStream(file)));

			while ((sCurrentLine = br.readLine()) != null) {
				set.add(sCurrentLine.trim());
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		return (set);
	}
}
