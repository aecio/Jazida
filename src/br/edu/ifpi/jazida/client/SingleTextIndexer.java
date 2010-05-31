package br.edu.ifpi.jazida.client;


import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import br.edu.ifpi.jazida.api.IOpalaTextIndexer;
import br.edu.ifpi.jazida.wrapper.MetaDocumentWrapper;
import br.edu.ifpi.opala.utils.MetaDocument;

public class SingleTextIndexer {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		InetSocketAddress addr = new InetSocketAddress("localhost", 16000);  // the server's inetsocketaddress
		
		IOpalaTextIndexer client = (IOpalaTextIndexer) RPC.waitForProxy(
				IOpalaTextIndexer.class, IOpalaTextIndexer.versionID, addr, conf);
		
		MetaDocument metadoc = new MetaDocument();
		metadoc.setAuthor("Aécio Santos");
		metadoc.setTitle("Um sistema de busca e indexação escalável para bibliotecas digitais");
		
		IntWritable code = client.addText(new MetaDocumentWrapper(metadoc), new Text("conteudo do documento do meu artigo! =)"));
		
		System.out.println(code);
	}
}

