package com.hbase.api.utils;

import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * 解析properties文件
 * 
 * @author doublejia
 *
 */
public class PropertiesResolve {
	
	private static Map<String,ResourceBundle> cache = new Hashtable<String,ResourceBundle>();
	
	public synchronized static ResourceBundle getProperties(String propertieName){
		if(cache.containsKey(propertieName)){
			return cache.get(propertieName);
		}else{
			ResourceBundle config = ResourceBundle.getBundle(propertieName, Locale.getDefault());
			cache.put(propertieName, config);
			return config;
		}
	}
	
	
	public synchronized static String getProperty(String propertieName, String key) {
		return getProperties(propertieName).getString(key);
	}


}
