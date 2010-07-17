package br.edu.ifpi.jazida.node;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

import br.edu.ifpi.jazida.wrapper.MetaDocumentWrapper;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.indexing.TextIndexerImpl;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextIndexerServer implements IJazidaTextIndexer {

	public static void main(String[] args) throws InterruptedException {
		try {
			TextIndexerServer server = new TextIndexerServer("monica-desktop", 16000);
			server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Server _server;

	public void start() throws IOException, InterruptedException {
		_server.start();
		_server.join();
	}

	public TextIndexerServer(String serverName, int port) throws IOException {
		Configuration conf = new Configuration();
		_server = RPC.getServer(this, serverName, port, conf);
	}

	@Override
	public IntWritable addText(MetaDocumentWrapper metaDocWrapper, Text content) {
		MetaDocument metaDocument = metaDocWrapper.getMetaDoc();
		
		System.out.println("\nMetaDocument ID: " + metaDocument.getId());
		
		TextIndexer indexer = TextIndexerImpl.getTextIndexerImpl();
		ReturnMessage result = indexer.addText(metaDocument, content.toString());

		return new IntWritable(result.getCode());
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
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
