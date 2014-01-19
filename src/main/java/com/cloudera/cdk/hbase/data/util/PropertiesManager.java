package com.cloudera.cdk.hbase.data.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertiesManager {

	private static Logger log = Logger.getLogger(PropertiesManager.class);
	
	public static String getProperty(String propertyKey) throws IOException	{
		String value = null;
		log.debug("Attempting to load property " + propertyKey);
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("ecrr.properties");
		if (is != null) {
			Properties p = new Properties();
			p.load(is);
			value = p.getProperty(propertyKey);
			log.info("Successfully loaded property value " + value);
			System.out.println("Successfully loaded property value: " + value);
		}
		return value;
	}		

}
