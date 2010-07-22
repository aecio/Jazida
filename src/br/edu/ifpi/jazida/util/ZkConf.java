package br.edu.ifpi.jazida.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Centraliza as configurações utilizadas no Jazida.
 * 
 * @author Aécio Solano Rodrigues Santos
 *
 */
public class ZkConf {
	
	private static final Logger LOG = Logger.getLogger(ZkConf.class);
	
	public static final String ZOOKEEPER_SERVERS;

	static {
		ZOOKEEPER_SERVERS = readZkServiceConf();
	}
	
	private static String readZkServiceConf(){
		String zkServers = null;
		try{
			File arquivo = new File("./conf/zkservice");
			BufferedReader reader = new BufferedReader(new FileReader(arquivo));
			
			StringBuffer buffer = new StringBuffer();
			String host = null;
			while((host = reader.readLine()) != null){
				if(!host.trim().startsWith("#")){
					buffer.append(',');
					buffer.append(host);
				}
			}
			zkServers = buffer.toString().replaceFirst(",", "");
			
		}catch (IOException e) {
			LOG.error("Falha na leitura do arquivo de configurações /conf/zkservice");
		}
		if(zkServers == null){
			return "localhost";
		}else {
			return zkServers;
		}
		
	}
	
}
