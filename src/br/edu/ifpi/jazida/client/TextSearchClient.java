package br.edu.ifpi.jazida.client;

import static br.edu.ifpi.jazida.writable.WritableUtils.convertIntToIntWritable;
import static br.edu.ifpi.jazida.writable.WritableUtils.convertMapToMapWritable;
import static br.edu.ifpi.jazida.writable.WritableUtils.convertStringListToTextArray;
import static br.edu.ifpi.jazida.writable.WritableUtils.convertStringToText;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.ITextSearchProtocol;
import br.edu.ifpi.jazida.zkservice.ZookeeperService;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.ReturnMessage;

/**
 * Interface de busca para usuários do Jazida. Aplicações devem utilizar essa
 * classe para realizar buscas em no Jazida.
 * 
 * @author Aécio Santos
 * 
 */
@Deprecated
public class TextSearchClient {

	private static final Logger LOG = Logger.getLogger(TextSearchClient.class);
	private static final Configuration HADOOP_CONF = new Configuration();
	private List<NodeStatus> datanodes;
	private Map<String, ITextSearchProtocol> proxyMap = new HashMap<String, ITextSearchProtocol>();
	private ZookeeperService zkService = new ZookeeperService();

	public TextSearchClient() throws KeeperException, InterruptedException, IOException {
		LOG.info("Inicializando TextSearchClient");

		this.datanodes = zkService.getDataNodes();

		if (datanodes.size()==0) {
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ZookeeperService.");
		}
		
		for (NodeStatus node : datanodes) {
			final InetSocketAddress hostAdress = new InetSocketAddress(
					node.getAddress(), node.getTextSearchServerPort());
			ITextSearchProtocol opalaClient = getTextSearchServer(hostAdress);
			proxyMap.put(node.getHostname(), opalaClient);
		}
	}

	private ITextSearchProtocol getTextSearchServer(
			final InetSocketAddress hostAdress) throws IOException {
		
		ITextSearchProtocol proxy = (ITextSearchProtocol) RPC.getProxy(
											ITextSearchProtocol.class,
											ITextSearchProtocol.versionID,
											hostAdress, HADOOP_CONF);
		return proxy;
	}

	public SearchResult search(	Map<String, String> fields,
								List<String> returnedFields,
								int batchStart,
								int batchSize,
								String sortOn,
								boolean reverse) throws IOException {
		
		LOG.info("Iniciando busca no TextSearchClient");
		
		SearchResult result = new SearchResult();
		result.setCodigo(ReturnMessage.SUCCESS);
		try {
			datanodes = zkService.getDataNodes();

			List<SearchResult> searchResults = new ArrayList<SearchResult>();
			//
			//	TODO: Paralelizar busca com ExecutorService's
			//
			for (NodeStatus datanode : datanodes) {
				SearchResult searchResult = proxyMap.get(datanode.getHostname()).search(
													convertMapToMapWritable(fields),
													convertStringListToTextArray(returnedFields),
													convertIntToIntWritable(batchStart),
													convertIntToIntWritable(batchSize),
													convertStringToText(sortOn),
													new BooleanWritable(reverse)
									).getSearchResult();
				
				searchResults.add(searchResult);
			}
			
			List<ResultItem> retornos = new ArrayList<ResultItem>();
			for (SearchResult searchResult : searchResults) {
				retornos.addAll(searchResult.getItems());
			}
			result.setItems(retornos);
			
			LOG.info("Busca finalizada no TextSearchClient");
		} catch (KeeperException e) {
			LOG.info("Não foi possível recuperar os DataNodes ativos no ZookeeperService");
		} catch (InterruptedException e) {
			LOG.info("Não foi possível recuperar os DataNodes ativos no ZookeeperService");
		}
		return result;
	}
}