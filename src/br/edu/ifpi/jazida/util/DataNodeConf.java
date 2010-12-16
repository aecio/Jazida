package br.edu.ifpi.jazida.util;

import br.edu.ifpi.jazida.node.DataNode;

/**
 * Centraliza as configurações de um {@link DataNode}.
 * 
 * @author Aécio Santos
 */
public class DataNodeConf {
	public static final String DATANODES_PATH ="/jazida";
	
	public static final int TEXT_INDEXER_SERVER_PORT = 16001;
	public static final int TEXT_SEARCH_SERVER_PORT = 16002;
	
	public static final int IMAGE_INDEXER_SERVER_PORT = 17001;
	public static final int IMAGE_SEARCH_SERVER_PORT = 17002;
}
