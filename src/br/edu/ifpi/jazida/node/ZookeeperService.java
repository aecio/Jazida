package br.edu.ifpi.jazida.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import br.edu.ifpi.jazida.nio.Serializer;
import br.edu.ifpi.jazida.util.ConnectionWatcher;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.ZkConf;

/**
 * Realiza a conexão do Jazida com o serviço do Zookeeper.
 * 
 * @author Aécio Solano Rodrigues Santos
 * 
 */
public class ZookeeperService extends ConnectionWatcher {

	private static final Logger LOG = Logger.getLogger(ZookeeperService.class);

	/**
	 * Construtor padrão. Conecta-se aos servidores do Zookeeper listados em
	 * {@link ZkConf}.ZOOKEEPER_SERVERS.
	 */
	public ZookeeperService() {
		try {
			super.connect(ZkConf.ZOOKEEPER_SERVERS);
		} catch (Exception e) {
			LOG.error(e);
		}
	}
	
	public ZookeeperService(ZooKeeper zk) {
		super.zk = zk;
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

		List<String> nodesIds = zk.getChildren(DataNodeConf.DATANODES_PATH, false);
		LOG.info(nodesIds.size() + " datanode(s) ativo(s) no momento.");

		List<NodeStatus> datanodes = new ArrayList<NodeStatus>();
		for (String node : nodesIds) {
			try {
				byte[] bytes = zk.getData(DataNodeConf.DATANODES_PATH + "/"
						+ node, false, null);
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
