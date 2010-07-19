package br.edu.ifpi.jazida.extras;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import br.edu.ifpi.jazida.node.ITextIndexerServer;
import br.edu.ifpi.jazida.wrapper.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.MetaDocument;

public class JazidaTextIndexer {

	static String[] servers = { "mario-desktop", "luigi-desktop", "monica-desktop"};
	static ITextIndexerServer[] clientes;
	
	private static File pasta = new File("/home/yoshi/dados-teste/mil-txt");
	private static File[] arquivos = pasta.listFiles();
	private static int atual = -1;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, SecurityException, NoSuchMethodException {
		
		long inicio = System.currentTimeMillis();
		
		try {
	
		clientes = new ITextIndexerServer[servers.length];
		Configuration configuracao = new Configuration();
		InetSocketAddress[] enderecosSocket = new InetSocketAddress[servers.length];
		
		for (int s=0; s<servers.length; s++) {
			//Criar um socket para esse DataNode
			enderecosSocket[s] = new InetSocketAddress(servers[s], 16000);
		}
		
		File arquivo = nextDocument();
		while( arquivo != null ) {
			
			Object[][] parametrosEnviados = new Object[servers.length][2];
			
			for (int s=0; s<servers.length; s++) {
				
				if( arquivo != null ){
				
					MetaDocument metadoc = new MetaDocument();
					metadoc.setTitle(arquivo.getName());
					metadoc.setId(arquivo.getName());
	
					//Ler conteúdo do arquivo
					FileInputStream stream;
					stream = new FileInputStream(arquivo);
					InputStreamReader streamReader = new InputStreamReader(stream);
					BufferedReader reader = new BufferedReader(streamReader);
	
					StringBuffer stringBuffer = new StringBuffer();
					String line;
					while ((line = reader.readLine()) != null) {
						stringBuffer.append(line);
					}
	
					parametrosEnviados[s][0] = new MetaDocumentWritable(metadoc);
					parametrosEnviados[s][1] = new Text(stringBuffer.toString());
					
				}
				
				arquivo = nextDocument();
			}
			
			Class<?>[] parametrosMetodos = {MetaDocumentWritable.class, Text.class};
			
			
			Object[] resposta = RPC.call(ITextIndexerServer.class.getMethod("addText", parametrosMetodos),
						parametrosEnviados,
						enderecosSocket,
						configuracao);
			for (int i = 0; i < resposta.length; i++) {
				System.out.println(servers[i] +" --> "+ resposta[i].toString() );
			}
			
		}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		long fim = System.currentTimeMillis();
		long total = fim - inicio;
		
		System.out.println("Tempo:" + total/1000.0 );
	
	}

	private static File nextDocument() {
		atual++;
		if (atual < arquivos.length) {
			return arquivos[atual];
		}else{
			return null; 
		}
	}
}

