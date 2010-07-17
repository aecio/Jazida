package br.edu.ifpi.jazida.util;

public class Configuration {
	public static final String DATANODES_PATH;
	public static final String ZOOKEEPER_SERVERS;

	static {
		DATANODES_PATH = "/jazida";
		ZOOKEEPER_SERVERS = "localhost"; 
	}
	
	static private int namesCounter = 0;
	static public String getServerName(){
		String[] names = {"fire","water","air","ice","earth","wind"};
		return names[namesCounter++];
	}
}
