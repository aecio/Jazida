package br.edu.ifpi.jazida.server;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import br.edu.ifpi.jazida.api.IOpalaTextIndexer;
import br.edu.ifpi.jazida.wrapper.MetaDocumentWrapper;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.indexing.TextIndexerImpl;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextIndexerServer implements IOpalaTextIndexer {

	public static void main(String[] args) throws InterruptedException {
		try {
			TextIndexerServer server = new TextIndexerServer();
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

	public TextIndexerServer() throws IOException {
		Configuration conf = new Configuration();
		_server = RPC.getServer(this, "monica-desktop", 16000, conf);
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

}
