package edu.mum.crystalball.mapreduce.utils;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * 
 * @author sachinkeshav
 *
 */
public class ResourceUtils {

	public static String getFilePath(String fileName) {
        URL url = ResourceUtils.class.getClassLoader().getResource(fileName);
        System.out.println("fileName="+fileName);
        System.out.println("url:" + url);
        String name = url.toString().replace("file:/", "/");
        return name.replace("jar:", "");
    }

    public static Properties loadProperties(String fileName) {
        Properties prop = new Properties();
        if(fileName==null||fileName.isEmpty()){
            return prop;//empty
        }
        try {
            if (fileName.startsWith("classPath:")) {
                InputStream stream = ResourceUtils.class.getClassLoader().getResourceAsStream(fileName.substring("classPath:".length()));
                prop.load(stream);

            } else {
                FileInputStream stream = new FileInputStream(fileName);
                DataInputStream in = new DataInputStream(stream);
                prop.load(in);
            }
            return prop;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String getCycleEndDate(String fileName){
        Properties prop = loadProperties(fileName);
        return prop.get("cycleEndDate").toString();
    }
    public static String getCycleStartDate (String fileName){
        Properties prop = loadProperties(fileName);
        return prop.get("cycleStartDate").toString();
    }
}
