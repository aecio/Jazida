package br.edu.ifpi.jazida.node;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import br.edu.ifpi.jazida.util.DataNodeConf;

/**
 * Servidor de RPC para a interface {@link ITextIndexerProtocol}. Implementa os
 * métodos de indexação de texto oferecidos pela Opala.
 * 
 * @author Aécio Santos
 */
public class TextIndexerServer {

	private Server server;
	private Configuration HADOOP_CONF = new Configuration();

	/**
	 * Inicia um servidor RPC para interface {@link ITextIndexerProtocol} no
	 * host local, na porta configurada em DataNodeConf.DEFAULT_PORT.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TextIndexerServer server = new TextIndexerServer(
					InetAddress.getLocalHost().getHostName(),
					DataNodeConf.TEXT_INDEXER_SERVER_PORT);
			server.start(true);
		} catch (Exception e) {
			e.printStackTrace();
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
	public TextIndexerServer(String serverName, int port) throws IOException {
		server = RPC.getServer(new TextIndexerProtocol(), serverName, port,
				HADOOP_CONF);
	}

	/**
	 * Inicia um servidor RPC para interface {@link ITextIndexerProtocol} no
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
