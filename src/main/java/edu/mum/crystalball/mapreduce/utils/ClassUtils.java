package edu.mum.crystalball.mapreduce.utils;

/**
 * 
 * @author sachinkeshav
 *
 */
public class ClassUtils {

	@SuppressWarnings("rawtypes")
	public static Object getInstance(Class cls) {
		try {
			return cls.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
