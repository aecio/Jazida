package br.edu.ifpi.jazida.node;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import br.edu.ifpi.jazida.node.protocol.IImageIndexerProtocol;

public class ImageSearcherServer {

	private static final Configuration HADOOP_CONF = new Configuration();
	private Server server;	

	public ImageSearcherServer(String serverName, int port) throws IOException {
		server = RPC.getServer(new ImageSearcherProtocol(), serverName, port, HADOOP_CONF);
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
