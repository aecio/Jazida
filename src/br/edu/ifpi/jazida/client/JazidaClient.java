package br.edu.ifpi.jazida.client;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.Configuration;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.zoo.ConnectionWatcher;

public class JazidaClient extends ConnectionWatcher {

	private Logger LOG = Logger.getLogger(JazidaClient.class);
	private PartitionPolicy partitionPolicy;

	public void start() throws IOException, InterruptedException,
			KeeperException {
		
		LOG.info("-----------------------------------");
		LOG.info("Conectando-se ao Zookeeper Service");
		LOG.info("-----------------------------------");
		
		super.connect(Configuration.ZOOKEEPER_SERVERS);
		
		List<String> nodesIds = zk.getChildren(Configuration.DATANODES_PATH, false);
		
		for (String node : nodesIds) {
			String path = Configuration.DATANODES_PATH + "/" + node;
			byte[] nodeBytes = zk.getData(path, false, null);
			
			try {
				NodeStatus status = (NodeStatus) Serializer.toObject(nodeBytes);
				System.out.println(status.getHostname());
				System.out.println(status.getIp());
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
	}

	public static void main(String[] args)
	throws IOException, InterruptedException, KeeperException {
		new JazidaClient().start();
	}
}

