package br.edu.ifpi.jazida.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.node.IJazidaTextIndexer;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.ZookeeperService;
import br.edu.ifpi.jazida.wrapper.MetaDocumentWrapper;
import br.edu.ifpi.jazida.zoo.ConnectionWatcher;
import br.edu.ifpi.opala.utils.MetaDocument;

public class JazidaClient extends ConnectionWatcher {

	private static final Logger LOG = Logger.getLogger(JazidaClient.class);
	private static PartitionPolicy<NodeStatus> partitionPolicy;
	private List<NodeStatus> datanodes;
	private Map<String, IJazidaTextIndexer> clientes = new HashMap<String, IJazidaTextIndexer>();
	private ZookeeperService zkService = new ZookeeperService();
	private final Configuration hadoopConf = new Configuration();

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		MetaDocument metadoc = new MetaDocument();
		JazidaClient cliente = new JazidaClient();
		cliente.addText(metadoc, "adfasdfasfas fas dfas dfas");
	}

	public JazidaClient() throws KeeperException, InterruptedException, IOException{
		this.datanodes = zkService.getDataNodes();
		
		for (NodeStatus node : datanodes) {
			// Inicializar proxy para servidor RPC
			final InetSocketAddress endereco = new InetSocketAddress(node
					.getHostname(), node.getPort());

			IJazidaTextIndexer opalaClient = (IJazidaTextIndexer) RPC.getProxy(
					IJazidaTextIndexer.class, IJazidaTextIndexer.versionID,
					endereco, hadoopConf);

			clientes.put(node.getHostname(), opalaClient);
		}
		
		partitionPolicy = new RoundRobinPartitionPolicy(datanodes);
	}

	public int addText(MetaDocument metaDocument, String content)
			throws IOException {
		MetaDocumentWrapper documentWrap = new MetaDocumentWrapper(metaDocument);
		NodeStatus node = partitionPolicy.nextNode();

		LOG.info(node.getHostname() + ": documento indexado: "+metaDocument.getId());

		IJazidaTextIndexer proxy = clientes.get(node.getHostname());
		IntWritable result = proxy.addText(documentWrap, new Text(content));

		return result.get();
	}

}
