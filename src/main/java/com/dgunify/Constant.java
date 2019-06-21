package com.dgunify;

import java.util.Properties;

public class Constant {
	
	public static String redis_host = "10.1.38.254";
	public static String redis_port = "8660";
	
	public static Properties connectionPropertiesMysql;
	public static Properties connectionPropertiesOracle;
	public static String mysqlUrl = "jdbc:mysql://10.1.35.254:3306/yddate_center?useUnicode=true&characterEncoding=utf-8";
	public static String oracleUrl = "jdbc:oracle:thin:@10.1.37.254:1521:orcl";
	static {
		connectionPropertiesMysql = new Properties();
		connectionPropertiesMysql.put("user", "hive");
		connectionPropertiesMysql.put("password","hxzk#2019$0422");
		connectionPropertiesMysql.put("driver", "com.mysql.jdbc.Driver");
		
		connectionPropertiesOracle = new Properties();
		connectionPropertiesOracle.put("user", "fwpt");
		connectionPropertiesOracle.put("password","fwpt123");
		connectionPropertiesOracle.put("driver", "oracle.jdbc.driver.OracleDriver");
	}
}
