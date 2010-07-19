package br.edu.ifpi.jazida.node;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import br.edu.ifpi.jazida.util.Configuration;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.zoo.ConnectionWatcher;

public class DataNode extends ConnectionWatcher {

	Logger LOG = Logger.getLogger(DataNode.class);
	Integer mutex = new Integer(0);
	private TextIndexerServer server;

	public void start(boolean lock) throws IOException, InterruptedException,
			KeeperException {
		
		LOG.info("-----------------------------------");
		LOG.info("Conectando-se ao Zookeeper Service");
		LOG.info("-----------------------------------");
		
		super.connect(Configuration.ZOOKEEPER_SERVERS);

		NodeStatus node = new NodeStatus();
		node.setHostname(InetAddress.getLocalHost().getHostName());
		node.setAddress(InetAddress.getLocalHost().getHostAddress());
		node.setPort(16000);

		String path = Configuration.DATANODES_PATH +"/"+ InetAddress.getLocalHost().getHostName();
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

	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException {
		new DataNode().start(true);
	}
}
