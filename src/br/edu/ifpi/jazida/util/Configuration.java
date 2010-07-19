package br.edu.ifpi.jazida.util;

/**
 * Centraliza as configurações utilizadas no Jazida.
 * 
 * @author Aécio Solano Rodrigues Santos
 *
 */
public class Configuration {
	
	public static final String DATANODES_PATH;
	public static final String ZOOKEEPER_SERVERS;
	public static final int DEFAULT_PORT;

	static {
		DATANODES_PATH = "/jazida";
		ZOOKEEPER_SERVERS = "localhost";
		DEFAULT_PORT = 16000;
	}
	
	static private int namesCounter = 0;
	static public String getServerName(){
		String[] names = {"fire","water","air","ice","earth","wind"};
		return names[namesCounter++];
	}
}
