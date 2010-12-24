package br.edu.ifpi.jazida.client;

import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.IImageIndexerProtocol;
import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.jazida.zkservice.ZookeeperService;
import br.edu.ifpi.opala.indexing.ImageIndexer;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageIndexerClient implements ImageIndexer {

	private static final Logger LOG = Logger.getLogger(ImageIndexerClient.class);
	private static Configuration HADOOP_CONFIGURATION = new Configuration();
	private HashMap<String,IImageIndexerProtocol> proxyMap = new HashMap<String, IImageIndexerProtocol>();
	private ZookeeperService zkService;

	public ImageIndexerClient() throws KeeperException, InterruptedException, IOException {
		zkService = new ZookeeperService(new RoundRobinPartitionPolicy());
		List<NodeStatus> datanodes = zkService.getDataNodes();
		if (datanodes.size()==0) 
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ZookeeperService.");
		
		for (NodeStatus node : datanodes) {
			final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(),
																		 node.getImageIndexerServerPort());
			IImageIndexerProtocol opalaClient = getImageIndexerProxy(socketAdress);
			proxyMap.put(node.getHostname(), opalaClient);
		}
	}
	
	private IImageIndexerProtocol getImageIndexerProxy(final InetSocketAddress endereco)
	throws IOException {
		IImageIndexerProtocol proxy = (IImageIndexerProtocol) RPC.getProxy(
											IImageIndexerProtocol.class,
											IImageIndexerProtocol.versionID,
											endereco, HADOOP_CONFIGURATION);
		return proxy;
	}

	@Override
	public ReturnMessage addImage(MetaDocument metaDocument, BufferedImage image) {
		NodeStatus node = zkService.nextNode();

		LOG.info(node.getHostname()+": imagem indexada: "+metaDocument.getId());

		IImageIndexerProtocol proxy = proxyMap.get(node.getHostname());
		IntWritable result = proxy.addImage(new MetaDocumentWritable(metaDocument),
											new BufferedImageWritable(image));
		
		return ReturnMessage.getReturnMessage(result.get());
	}

	@Override
	public ReturnMessage delImage(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void optimize() throws CorruptIndexException,
			LockObtainFailedException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void backupNow() throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void restoreBackup() {
		// TODO Auto-generated method stub

	}

	@Override
	public ReturnMessage updateImage(String id, Map<String, String> metaDocument) {
		// TODO Auto-generated method stub
		return null;
	}

}
