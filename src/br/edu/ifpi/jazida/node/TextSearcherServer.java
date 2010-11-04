package br.edu.ifpi.jazida.node;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

import br.edu.ifpi.jazida.node.protocol.ITextSearchProtocol;
import br.edu.ifpi.jazida.node.protocol.TextSearchProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;

@Deprecated
public class TextSearcherServer  {
	
	private Logger LOG = Logger.getLogger(TextSearcherServer.class);
	private Server searchServer;
	private static final Configuration HADOOP_CONF = new Configuration();
	
	/**
	 * Inicia um servidor RPC para interface {@link ITextSearchProtocol} no host
	 * local, na porta configurada em DataNodeConf.DEFAULT_PORT.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TextSearcherServer server = new TextSearcherServer(
							new TextSearchProtocol(),
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
	public TextSearcherServer(ITextSearchProtocol searchProtocol, String serverName, int port) throws IOException {
		LOG.info("Iniciando servidor de RPC TextSearcherServer na porta "+port);
		this.searchServer = RPC.getServer(searchProtocol, serverName, port, HADOOP_CONF);
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
