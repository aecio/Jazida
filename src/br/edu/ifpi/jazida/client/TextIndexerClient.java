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

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.ITextIndexerProtocol;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.ZookeeperService;
import br.edu.ifpi.jazida.util.ConnectionWatcher;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.MetaDocument;

/**
 * Interface para usuário do Jazida. Aplicações devem utilizar essa classe para
 * comunicação com um cluster Jazida.
 * 
 * @author Aécio Santos
 * 
 */
public class TextIndexerClient extends ConnectionWatcher {

	private static final Logger LOG = Logger.getLogger(TextIndexerClient.class);
	private static PartitionPolicy<NodeStatus> partitionPolicy;
	private List<NodeStatus> datanodes;
	private Map<String, ITextIndexerProtocol> proxyMap = new HashMap<String, ITextIndexerProtocol>();
	private ZookeeperService zkService = new ZookeeperService();
	private final Configuration hadoopConf = new Configuration();

	public TextIndexerClient() throws KeeperException, InterruptedException, IOException {
		
		this.datanodes = zkService.getDataNodes();
		if (datanodes.size()==0) 
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ZookeeperService.");
		
		for (NodeStatus node : datanodes) {
			final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(),
																		 node.getTextIndexerServerPort());
			ITextIndexerProtocol opalaClient = getTextIndexerServer(socketAdress);
			proxyMap.put(node.getHostname(), opalaClient);
		}
		
		partitionPolicy = new RoundRobinPartitionPolicy(datanodes);
	}

	private ITextIndexerProtocol getTextIndexerServer(final InetSocketAddress endereco)
	throws IOException {
		ITextIndexerProtocol proxy = (ITextIndexerProtocol) RPC.getProxy(
											ITextIndexerProtocol.class,
											ITextIndexerProtocol.versionID,
											endereco, hadoopConf);
		return proxy;
	}

	public int addText(MetaDocument metaDocument, String content)
	throws IOException {
		
		MetaDocumentWritable documentWrap = new MetaDocumentWritable(metaDocument);
		NodeStatus node = partitionPolicy.nextNode();

		LOG.info(node.getHostname()+": documento indexado: "+metaDocument.getId());

		ITextIndexerProtocol proxy = proxyMap.get(node.getHostname());
		IntWritable result = proxy.addText(documentWrap, new Text(content));

		return result.get();
	}

}
