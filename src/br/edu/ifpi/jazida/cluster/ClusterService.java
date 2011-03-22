package br.edu.ifpi.jazida.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import br.edu.ifpi.jazida.client.PartitionPolicy;
import br.edu.ifpi.jazida.client.RoundRobinPartitionPolicy;
import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.util.ZkConf;

/**
 * Realiza a conexão do Jazida com o serviço do Zookeeper.
 * 
 * @author Aécio Solano Rodrigues Santos
 * 
 */
public class ClusterService extends ConnectionWatcher {

	private static final Logger LOG = Logger.getLogger(ClusterService.class);
	private PartitionPolicy<NodeStatus> partitionPolicy;

	public ClusterService() {
		this(new RoundRobinPartitionPolicy());
	}
	
	public ClusterService(ZooKeeper zk) {
		super(zk);
	}

	/**
	 * Construtor padrão. Conecta-se aos servidores do Zookeeper listados em
	 * {@link ZkConf}.ZOOKEEPER_SERVERS.
	 */
	public ClusterService(PartitionPolicy<NodeStatus> partitionPolicy) {
		try {
			super.connect(ZkConf.ZOOKEEPER_SERVERS);
			List<NodeStatus> nodes = getDataNodes();
			this.partitionPolicy = partitionPolicy;
			this.partitionPolicy.addNode(nodes.toArray(new NodeStatus[nodes.size()]));
		} catch (Exception e) {
			LOG.error(e);
		}
	}
	
	public void registerOnZookepper(String hostName, NodeStatus node)
	throws KeeperException, InterruptedException, IOException {
		LOG.info("Conectando-se ao Zookeeper Service...");
		if (zk.exists(ZkConf.DATANODES_PATH, false) == null) {
			
			zk.create(	ZkConf.DATANODES_PATH, 
						null, 
						Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			
		}
		String path = ZkConf.DATANODES_PATH + "/" + hostName;
		String createdPath = zk.create(	path,
										Serializer.fromObject(node),
										Ids.OPEN_ACL_UNSAFE, 
										CreateMode.EPHEMERAL);
		
		LOG.info("Conectado ao grupo: " + createdPath);
	}


	/**
	 * Lista os {@link DataNode}s conectados no momento ao ZookeeperService.
	 * 
	 * @return datanodes
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public List<NodeStatus> getDataNodes() throws KeeperException,
			InterruptedException, IOException {
		/*
		 * TODO: Implementar "cache" de datanodes, evitando fazer uma chamada ao
		 * Zookeeper toda vez que esse método for chamado. Mater uma lista com
		 * os dadanodes e remover da lista quando receber notificações de
		 * desconexão do datanode do Zookeeper.
		 */
		List<String> nodesIds = zk.getChildren(ZkConf.DATANODES_PATH, false);
		LOG.info(nodesIds.size() + " datanode(s) ativo(s) no momento.");

		List<NodeStatus> datanodes = new ArrayList<NodeStatus>();
		for (String node : nodesIds) {
			try {
				byte[] bytes = zk.getData(ZkConf.DATANODES_PATH + "/" + node, false, null);
				NodeStatus status = (NodeStatus) Serializer.toObject(bytes);
				LOG.info(status.getHostname());
				datanodes.add(status);

			} catch (ClassNotFoundException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		return datanodes;
	}
	
	public NodeStatus nextNode() {
		return partitionPolicy.nextNode();
	}

}
