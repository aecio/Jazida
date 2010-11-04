package br.edu.ifpi.jazida.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

import br.edu.ifpi.jazida.node.protocol.ITextSearchableProtocol;
import br.edu.ifpi.jazida.node.protocol.TextSearchableProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.opala.utils.Path;

public class TextSearchableServer {
	
	private Logger LOG = Logger.getLogger(TextSearchableServer.class);
	private Server searchServer;
	private static final Configuration HADOOP_CONF = new Configuration();
	
	/**
	 * Inicia um servidor RPC para interface {@link ITextSearchableProtocol} no host
	 * local, na porta configurada em DataNodeConf.DEFAULT_PORT.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			FSDirectory dir = FSDirectory.open(new File(Path.TEXT_INDEX.getValue()));
			IndexSearcher searcher = new IndexSearcher(dir, true);
			TextSearchableServer server = new TextSearchableServer(
							new TextSearchableProtocol(searcher),
							InetAddress.getLocalHost().getHostName(),
							DataNodeConf.TEXT_SEARCH_SERVER_PORT );
			server.start(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Contrutor do servidor. Receber o nome do servidor e a porta em que irar
	 * escutar como parametros.
	 * 
	 * @param serverName O nome do servidor RPC
	 * @param port A port que o servidor receberá chamadas RPC
	 * @throws IOException
	 */
	public TextSearchableServer(ITextSearchableProtocol searchableProtocol, String serverName, int port) throws IOException {
		LOG.info("Iniciando servidor de RPC SearchableServer na porta "+port);
		this.searchServer = RPC.getServer(searchableProtocol, serverName, port, HADOOP_CONF);
	}

	/**
	 * Inicia um servidor RPC para interface {@link ITextSearchProtocol} no host
	 * e porta passados no construtor. Se o paramêtro join for igual a true, a
	 * Thread ficará bloqueada até que seja encerrada. Se for false, o servidor
	 * será iniciado e a execução continuará normalmente.
	 * 
	 * @param join
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void start(boolean join) throws IOException, InterruptedException {
		searchServer.start();
		if (join) {
			searchServer.join();
		}
	}

	public void stop() {
		searchServer.stop();
	}

}
