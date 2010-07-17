package br.edu.ifpi.jazida.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.util.Configuration;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.zoo.ConnectionWatcher;

public class ZookeeperService extends ConnectionWatcher {
	
	private static final Logger LOG = Logger.getLogger(ZookeeperService.class);
	
	public ZookeeperService(){
		LOG.info("-----------------------------------");
		LOG.info("Conectando-se ao ZookeeperService...");
		try{
			super.connect(Configuration.ZOOKEEPER_SERVERS);
		}catch (Exception e) {
			LOG.error(e);
		}
		LOG.info("Conectado.");
		LOG.info("-----------------------------------");
	}

	public List<NodeStatus> getDataNodes() throws KeeperException, InterruptedException, IOException {
		
		List<String> nodesIds = zk.getChildren(Configuration.DATANODES_PATH, false);
		LOG.info(nodesIds.size() + " datanode(s) ativo(s) no momento.");
		
		List<NodeStatus> datanodes = new ArrayList<NodeStatus>();
		for (String node : nodesIds) {
			try {
				
				byte[] bytes = zk.getData(Configuration.DATANODES_PATH+"/"+node, false, null);
				NodeStatus status = (NodeStatus) Serializer.toObject(bytes);
				LOG.info(status.getHostname());
				datanodes.add(status);
				
			} catch (ClassNotFoundException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return datanodes;
	}
}