package br.edu.ifpi.jazida.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import br.edu.ifpi.jazida.node.protocol.ImageIndexerProtocol;
import br.edu.ifpi.jazida.node.protocol.ImageSearcherProtocol;
import br.edu.ifpi.jazida.node.protocol.TextIndexerProtocol;
import br.edu.ifpi.jazida.node.protocol.TextSearchableProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.util.ZkConf;
import br.edu.ifpi.jazida.zkservice.ConnectionWatcher;
import br.edu.ifpi.opala.utils.Path;

/**
 * Representa um nó conectado ao cluster Jazida. Durante sua inicialização,
 * publica-se no serviço do Zookeeper como disponível e inicializa os servidores
 * de chamada de procedimento remoto (RPC) nas portas especificadas.
 * 
 * @author Aécio Santos
 * 
 */
public class DataNode extends ConnectionWatcher {

	private static final Logger LOG = Logger.getLogger(DataNode.class);

	private RPCServer textIndexerServer;
	private RPCServer textSearchableServer;
	private RPCServer imageIndexerServer;
	private RPCServer imageSearcherServer;
	
	/**
	 * Inicia um {@link DataNode} com configurações do host local.
	 * 
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start() throws UnknownHostException, IOException,
	InterruptedException, KeeperException {
		
		this.start(	InetAddress.getLocalHost().getHostName(), 
				InetAddress.getLocalHost().getHostAddress(),
				DataNodeConf.TEXT_INDEXER_SERVER_PORT,
				DataNodeConf.TEXT_SEARCH_SERVER_PORT,
				DataNodeConf.IMAGE_INDEXER_SERVER_PORT,
				DataNodeConf.IMAGE_SEARCH_SERVER_PORT,
				true);
	}

	/**
	 * Inicia um {@link DataNode} com configurações do host local.
	 * 
	 * @param lock
	 *   Se a execução deve ser bloqueada após a inicialização do {@link DataNode}.
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start(boolean lock) throws UnknownHostException, IOException,
											InterruptedException, KeeperException {

		this.start(	InetAddress.getLocalHost().getHostName(), 
					InetAddress.getLocalHost().getHostAddress(),
					DataNodeConf.TEXT_INDEXER_SERVER_PORT,
					DataNodeConf.TEXT_SEARCH_SERVER_PORT,
					DataNodeConf.IMAGE_INDEXER_SERVER_PORT,
					DataNodeConf.IMAGE_SEARCH_SERVER_PORT,
					lock);
	}

	/**
	 * Inicia um {@link DataNode} de acordo com os parâmetros recebidos.
	 * 
	 * @param hostName
	 *            O nome do host em que o DataNode está sendo iniciado.
	 * @param hostAddress
	 *            O endereço IP do host.
	 * @param textIndexerServerPort
	 *            O número da porta que o servidor escutará requisições.
	 * @param textSearchServerPort 
	 * @param lock
	 *            Se a execução será bloqueada após o inicialização do
	 *            {@link DataNode}
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start(String hostName, String hostAddress,
						int textIndexerServerPort,
						int textSearchServerPort,
						int imageIndexerServerPort,
						int imageSearchServerPort,
						boolean lock) 
	throws IOException, InterruptedException, KeeperException {

		LOG.info("-------------------------------------");
		LOG.info("Conectando-se ao Zookeeper Service...");

		super.connect(ZkConf.ZOOKEEPER_SERVERS);

		NodeStatus node = new NodeStatus(hostName, hostAddress, 
										textIndexerServerPort,
										textSearchServerPort,
										imageIndexerServerPort,
										imageSearchServerPort);

		registerOnZookepper(hostName, node);

		LOG.info("Iniciando o protocolo de RPC ImageIndexerServer");
		File imageIndexPath = new File(Path.IMAGE_INDEX.getValue());
		createIndexIfNotExists(imageIndexPath);
		
		imageIndexerServer = new RPCServer(new ImageIndexerProtocol(),
													node.getAddress(),
													node.getImageIndexerServerPort());
		imageIndexerServer.start(false);

		LOG.info("Iniciando o protocolo de RPC ImageSearchServer");
		imageSearcherServer = new RPCServer(new ImageSearcherProtocol(),
											node.getAddress(),
											node.getImageSearcherServerPort());
		imageSearcherServer.start(false);
		

		LOG.info("Iniciando o protocolo de RPC TextIndexerServer");
		textIndexerServer = new RPCServer(	new TextIndexerProtocol(),
											node.getAddress(),
											node.getTextIndexerServerPort());
		textIndexerServer.start(false);

		LOG.info("Iniciando o protocolo de RPC TextSearchableServer");
		File textIndexPath = new File(Path.TEXT_INDEX.getValue());
		createIndexIfNotExists(textIndexPath);
		FSDirectory dir = FSDirectory.open(textIndexPath);
		IndexSearcher searcher = new IndexSearcher(dir, true);
		textSearchableServer = new RPCServer(new TextSearchableProtocol(searcher),
												node.getAddress(),
												node.getTextSearchServerPort());
		textSearchableServer.start(lock);		
	}

	private void registerOnZookepper(String hostName, NodeStatus node)
			throws KeeperException, InterruptedException, IOException {
		
		if (zk.exists(DataNodeConf.DATANODES_PATH, false) == null) {
			
			zk.create(	DataNodeConf.DATANODES_PATH, 
						null, 
						Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			
		}

		String path = DataNodeConf.DATANODES_PATH + "/" + hostName;
		String createdPath = zk.create(path, Serializer.fromObject(node), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		LOG.info("Conectado ao grupo: " + createdPath);
	}

	private void createIndexIfNotExists(File indexPath) throws CorruptIndexException, LockObtainFailedException, IOException {
		IndexWriter indexWriter = new IndexWriter(	FSDirectory.open(indexPath),
													new BrazilianAnalyzer(Version.LUCENE_30),
													IndexWriter.MaxFieldLength.UNLIMITED);
		indexWriter.close();
	}
	
	public void stop() throws InterruptedException {
		textIndexerServer.stop();
		textSearchableServer.stop();
		imageIndexerServer.stop();
		imageSearcherServer.stop();
		super.disconnect();
	}
}
