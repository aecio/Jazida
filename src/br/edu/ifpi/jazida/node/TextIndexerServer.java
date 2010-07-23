package br.edu.ifpi.jazida.node;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.indexing.TextIndexerImpl;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

/**
 * Servidor de RPC para a interface {@link ITextIndexerServer}. Implementa os
 * métodos de indexação de texto oferecidos pela Opala.
 * 
 * @author aecio
 * 
 */
public class TextIndexerServer implements ITextIndexerServer {

	private Server server;

	/**
	 * Inicia um servidor RPC para interface {@link ITextIndexerServer} no host
	 * local, na porta 16000.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TextIndexerServer server = new TextIndexerServer(InetAddress
					.getLocalHost().getHostName(), 16000);
			server.start(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Inicia um servidor RPC para interface {@link ITextIndexerServer} no host
	 * e porta passados no construtor. Se o paramêtro join for igual a true, a
	 * Thread ficará bloqueada até que seja encerrada. Se for false, o servidor
	 * será iniciado e a execução continuará normalmente.
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
		Configuration conf = new Configuration();
		server = RPC.getServer(this, serverName, port, conf);
	}

	/* Métodos da interface implementada */

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public IntWritable addText(MetaDocumentWritable metaDocWrapper, Text content) {
		MetaDocument metaDocument = metaDocWrapper.getMetaDoc();
		TextIndexer indexer = TextIndexerImpl.getTextIndexerImpl();
		ReturnMessage result = indexer
				.addText(metaDocument, content.toString());
		return new IntWritable(result.getCode());
	}

	@Override
	public void backupNow() throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public IntWritable delText(Text identifier) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void optimize() throws CorruptIndexException,
			LockObtainFailedException, IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void restoreBackup() {
		// TODO Auto-generated method stub
	}

	@Override
	public IntWritable updateText(String id, Map<String, String> metaDocument) {
		// TODO Auto-generated method stub
		return null;
	}

}
