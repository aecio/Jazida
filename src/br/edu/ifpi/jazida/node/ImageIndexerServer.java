package br.edu.ifpi.jazida.node;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

import br.edu.ifpi.jazida.node.protocol.IImageIndexerProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;

public class ImageIndexerServer {
	
	private static final Configuration HADOOP_CONF = new Configuration();
	private static final Logger LOG = Logger.getLogger(ImageIndexerServer.class);
	private Server server;

	/**
	 * Inicia um servidor RPC para interface {@link IImageIndexerProtocol} no
	 * host local, na porta configurada em DataNodeConf.DEFAULT_PORT.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ImageIndexerServer server = new ImageIndexerServer(
					InetAddress.getLocalHost().getHostName(),
					DataNodeConf.IMAGE_INDEXER_SERVER_PORT);
			server.start(true);
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	/**
	 * Contrutor do servidor. Receber o nome do servidor e a porta em que irar
	 * escutar como parametros.
	 * 
	 * @param serverName
	 *            O nome do servidor RPC
	 * @param port
	 *            A port que o servidor receberá chamadas RPC
	 * @throws IOException
	 */
	public ImageIndexerServer(String serverName, int port) throws IOException {
		server = RPC.getServer(new ImageIndexerProtocol(), serverName, port, HADOOP_CONF);
	}

	/**
	 * Inicia um servidor RPC para interface {@link IImageIndexerProtocol} no
	 * host e porta passados no construtor. Se o paramêtro join for igual a
	 * true, a Thread ficará bloqueada até que seja encerrada. Se for false, o
	 * servidor será iniciado e a execução continuará normalmente.
	 * 
	 * @param join
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void start(boolean join) throws IOException, InterruptedException {
		server.start();
		if (join) {
			server.join();
		}
	}

	public void stop() {
		server.stop();
	}
}
