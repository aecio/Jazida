package br.edu.ifpi.jazida.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.ITextIndexerProtocol;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.jazida.writable.WritableUtils;
import br.edu.ifpi.jazida.zkservice.ConnectionWatcher;
import br.edu.ifpi.jazida.zkservice.ZookeeperService;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

/**
 * Interface para usuário do Jazida. Aplicações devem utilizar essa classe para
 * comunicação com um cluster Jazida.
 * 
 * @author Aécio Santos
 * 
 */
public class TextIndexerClient extends ConnectionWatcher implements TextIndexer {

	private static final Logger LOG = Logger.getLogger(TextIndexerClient.class);
	private static Configuration HADOOP_CONFIGURATION = new Configuration();
	private Map<String, ITextIndexerProtocol> proxyMap = new HashMap<String, ITextIndexerProtocol>();
	private ZookeeperService zkService;
	private ExecutorService threadPool;

	public TextIndexerClient() throws KeeperException, InterruptedException, IOException {
		zkService = new ZookeeperService(new RoundRobinPartitionPolicy());
		List<NodeStatus> datanodes = zkService.getDataNodes();
		if (datanodes.size()==0) 
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ZookeeperService.");
		
		for (NodeStatus node : datanodes) {
			final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(),
																		 node.getTextIndexerServerPort());
			ITextIndexerProtocol opalaClient = getTextIndexerServer(socketAdress);
			proxyMap.put(node.getHostname(), opalaClient);
		}
		threadPool = Executors.newCachedThreadPool();
	}

	private ITextIndexerProtocol getTextIndexerServer(final InetSocketAddress endereco)
	throws IOException {
		ITextIndexerProtocol proxy = (ITextIndexerProtocol) RPC.getProxy(
											ITextIndexerProtocol.class,
											ITextIndexerProtocol.versionID,
											endereco, HADOOP_CONFIGURATION);
		return proxy;
	}
	
	@Override
	public ReturnMessage addText(MetaDocument metaDocument, String content) {
		
		MetaDocumentWritable documentWrap = new MetaDocumentWritable(metaDocument);
		NodeStatus node = zkService.nextNode();

		ITextIndexerProtocol proxy = proxyMap.get(node.getHostname());
		IntWritable result = proxy.addText(documentWrap, new Text(content));
		
		LOG.info("Indexação de "+metaDocument.getId()+" em "+node.getHostname()
					+" retornou "+ReturnMessage.getReturnMessage(result.get()));
		
		return ReturnMessage.getReturnMessage(result.get());
	}

	@Override
	public ReturnMessage delText(final String identifier) {
		ReturnMessage message = ReturnMessage.ID_NOT_FOUND;
		try {
			ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
			for (final NodeStatus nodeStatus : zkService.getDataNodes()) {
				Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
					@Override
					public IntWritable call() throws Exception {
						ITextIndexerProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.delText(new Text(identifier));
					}
				});
				requests.add(request);
			}
			for (Future<IntWritable> future : requests) {
				IntWritable returnCode = future.get(3000, TimeUnit.MILLISECONDS);
				if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.SUCCESS){
					message = ReturnMessage.SUCCESS;
				}
			}			
		
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ExecutionException e) {
			LOG.error(e);
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		} catch (TimeoutException e) {
			LOG.error(e);
		}
		return message;
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
	public ReturnMessage updateText(final String id, final Map<String, String> metaDocument) {
		ReturnMessage message = ReturnMessage.ID_NOT_FOUND;
		try {
			ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
			for (final NodeStatus nodeStatus : zkService.getDataNodes()) {
				Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
					@Override
					public IntWritable call() throws Exception {
						ITextIndexerProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.updateText(new Text(id), WritableUtils.convertMapToMapWritable(metaDocument));
					}
				});
				requests.add(request);
			}
			for (Future<IntWritable> future : requests) {
				IntWritable returnCode = future.get(3000, TimeUnit.MILLISECONDS);
				if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.SUCCESS) {
					message = ReturnMessage.SUCCESS;
				}
			}			
		
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ExecutionException e) {
			LOG.error(e);
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		} catch (TimeoutException e) {
			LOG.error(e);
		}
		return message;
	}

}
