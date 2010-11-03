package br.edu.ifpi.jazida.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.ParallelMultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.ISearchableProtocol;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.ZookeeperService;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.searching.TextSearcher;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ParallelTextSearcherClient implements TextSearcher {
	
	private static final Logger LOG = Logger.getLogger(ParallelTextSearcherClient.class);
	private static final Configuration HADOOP_CONF = new Configuration();
	private ParallelMultiSearcher searcher;
	private List<NodeStatus> datanodes;
	private Map<String, ISearchableProtocol> proxyMap = new HashMap<String, ISearchableProtocol>();
	private ZookeeperService zkService = new ZookeeperService();
	
	public ParallelTextSearcherClient() throws IOException, KeeperException, InterruptedException {
		LOG.info("Inicializando ParallelMultiSearchClient");

		this.datanodes = zkService.getDataNodes();

		if (datanodes.size()==0) {
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ZookeeperService.");
		}
		RemoteSearchableAdapter[] searchables = new RemoteSearchableAdapter[datanodes.size()];
		int i=0;
		for (NodeStatus node : datanodes) {
			final InetSocketAddress hostAdress = new InetSocketAddress(
					node.getAddress(), node.getTextSearchServerPort()+1);
			ISearchableProtocol searchableClient = getSearchableProxy(hostAdress);
			proxyMap.put(node.getHostname(), searchableClient);
			searchables[i] = new RemoteSearchableAdapter(searchableClient);
			i++;
		}
		
		searcher = new ParallelMultiSearcher(searchables);
	}
	
	private ISearchableProtocol getSearchableProxy(
			final InetSocketAddress hostAdress) throws IOException {
		
		ISearchableProtocol proxy = (ISearchableProtocol) RPC.getProxy(
											ISearchableProtocol.class,
											ISearchableProtocol.versionID,
											hostAdress, HADOOP_CONF);
		return proxy;
	}

	@Override
	public SearchResult search(	Map<String, String> fields,
								List<String> returnedFields,
								int batchStart,
								int batchSize,
								String sortOn,
								boolean reverse) {
		
		if (fields == null || fields.size() == 0) {
			return new SearchResult(ReturnMessage.INVALID_QUERY, null);
		}
		
		try {
			int init = batchStart <= 0 ? 0 : batchStart - 1;
			int limit = batchSize <= 0 ? batchStart + 20 : batchStart + batchSize - 1;
			
			Sort sort = createSort(sortOn, reverse);
			Query query = createQuery(fields);
			
			ScoreDoc[] hits;
			if(sort == null) {
				hits = searcher.search(query, null, limit).scoreDocs;
			}else {
				hits = searcher.search(query, null, limit, sort).scoreDocs;
			}

			if (init >= hits.length) {
				return new SearchResult(ReturnMessage.EMPTY_SEARCHER, 
										new ArrayList<ResultItem>());
			}			
			
			return createResultItens(init, hits, returnedFields);
			
		} catch (ParseException e) {
			return new SearchResult(ReturnMessage.INVALID_QUERY, null);
		} catch (IOException e) {
			return new SearchResult(ReturnMessage.UNEXPECTED_INDEX_ERROR, null);
		}
	}

	@Override
	public SearchResult search( Map<String, String> fields,
								List<String> returnedFields,
								int batchStart,
								int batchSize,
								String sortOn) {
		
		return search(fields, returnedFields, batchStart, batchSize, sortOn, false);
	}
	

	private Sort createSort(String sortOn, boolean reverse)
	throws CorruptIndexException, IOException {
		Sort sort = null;
		SortField sf = null;
		if (sortOn != null && !sortOn.equals("")){
			sf = new SortField(sortOn, SortField.STRING, reverse);
			sort = new Sort(sf);
		}
		return sort;
	}
	
	private Query createQuery(Map<String, String> fields) throws ParseException {
		QueryParser queryParser = new QueryParser(Version.LUCENE_30, null, new BrazilianAnalyzer(Version.LUCENE_30));
		StringBuffer queryString = new StringBuffer();
		for (Map.Entry<String, String> entry : fields.entrySet()) {
			queryString.append(entry.getKey());
			queryString.append(":\"");
			queryString.append(entry.getValue());
			queryString.append("\" ");
		}
		String strQuery = null;
		try {
			strQuery = new String(queryString.toString().getBytes(),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.warn(e.getStackTrace());
		}
		return queryParser.parse(strQuery);
	}
	
	private SearchResult createResultItens( int init, 
											ScoreDoc[] hits,
											List<String> returnedFields) throws CorruptIndexException, IOException {
		
		ArrayList<ResultItem> items =  new ArrayList<ResultItem>();
		
		for (int i = init; i < hits.length; i++) {
			
			ResultItem resultItem = new ResultItem();
			resultItem.setId(searcher.doc(hits[i].doc).get(Metadata.ID.getValue()));
			resultItem.setScore(Float.toString(hits[i].score));
			
			if (i > 0 && resultItem.getScore().equals(Float.toString(hits[i-1].score))) {
				resultItem.setDuplicated(true);
			}

			Map<String, String> docFields = new HashMap<String, String>();
			if (returnedFields != null) {
				for (String returnedField : returnedFields) {
					if (searcher.doc(hits[i].doc).get(returnedField) != null) {
						docFields.put(returnedField, searcher.doc(hits[i].doc).get(returnedField));
					}
				}
			}
			resultItem.setFields(docFields);
			items.add(resultItem);
		}
		
		SearchResult result = new SearchResult(ReturnMessage.SUCCESS, items);
		return result;
	}
	
	public void close() throws InterruptedException {
		zkService.disconnect();
	}
}
