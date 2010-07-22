package br.edu.ifpi.jazida.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import br.edu.ifpi.jazida.util.ConnectionWatcher;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.util.ZkConf;

/**
 * Representa um nó conectado ao cluster Jazida.
 * @author aecio
 *
 */
public class DataNode extends ConnectionWatcher {

	private Logger LOG = Logger.getLogger(DataNode.class);
	private TextIndexerServer server;
	
	
	/**
	 * Inicia um {@link DataNode} com configurações do host local.
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		new DataNode().start();
	}

	/**
	 * Inicia um {@link DataNode} com configurações do host local.
	 * 
	 * @param lock - Se a execução será bloqueada após a inicialização do {@link DataNode}.
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start(boolean lock)
	throws UnknownHostException, IOException, InterruptedException, KeeperException{
		
		this.start( InetAddress.getLocalHost().getHostName(),
					InetAddress.getLocalHost().getHostAddress(), 
					DataNodeConf.DEFAULT_PORT,
					lock);
	}
	
	
	/**
	 * Inicia um {@link DataNode} com configurações do host local.
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start()
	throws UnknownHostException, IOException, InterruptedException, KeeperException{
		
		this.start( InetAddress.getLocalHost().getHostName(),
					InetAddress.getLocalHost().getHostAddress(), 
					DataNodeConf.DEFAULT_PORT,
					true);
	}
	
	
	/**
	 * Inicia um {@link DataNode} de acordo com os parâmetros recebidos.
	 * 
	 * @param hostName O nome do host em que o DataNode está sendo iniciado.
	 * @param hostAddress O endereço IP do host.
	 * @param port O número da porta que o servidor escutará requisições.
	 * @param lock Se a execução será bloqueada após o inicialização do {@link DataNode}
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start(String hostName, String hostAddress, int port, boolean lock)
	throws IOException, InterruptedException, KeeperException {
		
		LOG.info("-----------------------------------");
		LOG.info("Conectando-se ao Zookeeper Service...");
		LOG.info("-----------------------------------");
		
		super.connect(ZkConf.ZOOKEEPER_SERVERS);

		NodeStatus node = new NodeStatus();
		node.setHostname(hostName);
		node.setAddress(hostAddress);
		node.setPort(port);
		
		if (zk.exists(DataNodeConf.DATANODES_PATH, false) == null){
			zk.create( DataNodeConf.DATANODES_PATH, null,
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		String path = DataNodeConf.DATANODES_PATH +"/"+ InetAddress.getLocalHost().getHostName();
		String createdPath = zk.create( path,
										Serializer.fromObject(node),
										Ids.OPEN_ACL_UNSAFE,
										CreateMode.EPHEMERAL);
		
		LOG.info("----------------------------------------");
		LOG.info("Conectado ao grupo: " + createdPath);
		LOG.info("Agora, vou iniciar o servidor IPC/RPC");
		LOG.info("----------------------------------------");
		
		server = new TextIndexerServer(node.getHostname(), node.getPort());
		server.start(lock);
	}
}
