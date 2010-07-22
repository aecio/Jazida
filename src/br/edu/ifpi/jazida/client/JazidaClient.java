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

import br.edu.ifpi.jazida.node.ITextIndexerServer;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.ZookeeperService;
import br.edu.ifpi.jazida.util.ConnectionWatcher;
import br.edu.ifpi.jazida.wrapper.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.MetaDocument;

public class JazidaClient extends ConnectionWatcher {

	private static final Logger LOG = Logger.getLogger(JazidaClient.class);
	private static PartitionPolicy<NodeStatus> partitionPolicy;
	private List<NodeStatus> datanodes;
	private Map<String, ITextIndexerServer> clientes = new HashMap<String, ITextIndexerServer>();
	private ZookeeperService zkService = new ZookeeperService();
	private final Configuration hadoopConf = new Configuration();

	public static void main(String[] args)
	throws IOException, KeeperException, InterruptedException {
		MetaDocument metadoc = new MetaDocument();
		JazidaClient cliente = new JazidaClient();
		cliente.addText(metadoc, "adfasdfasfas fas dfas dfas");
	}

	public JazidaClient()
	throws KeeperException, InterruptedException, IOException {
		
		this.datanodes = zkService.getDataNodes();
		for (NodeStatus node : datanodes) {
			final InetSocketAddress endereco = new InetSocketAddress(node
					.getAddress(), node.getPort());

			ITextIndexerServer opalaClient = (ITextIndexerServer) RPC.getProxy(
					ITextIndexerServer.class, ITextIndexerServer.versionID,
					endereco, hadoopConf);

			clientes.put(node.getHostname(), opalaClient);
		}
		partitionPolicy = new RoundRobinPartitionPolicy(datanodes);
	}
	
	public int addText(MetaDocument metaDocument, String content)
			throws IOException {
		MetaDocumentWritable documentWrap = new MetaDocumentWritable(
				metaDocument);
		NodeStatus node = partitionPolicy.nextNode();

		LOG.info(node.getHostname() + ": documento indexado: "
				+ metaDocument.getId());

		ITextIndexerServer proxy = clientes.get(node.getHostname());
		IntWritable result = proxy.addText(documentWrap, new Text(content));

		return result.get();
	}

}
