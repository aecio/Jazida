package br.edu.ifpi.jazida;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import br.edu.ifpi.jazida.api.IOpalaTextIndexer;
import br.edu.ifpi.jazida.wrapper.MetaDocumentWrapper;
import br.edu.ifpi.opala.utils.MetaDocument;

public class ParallelIndexer implements Runnable {

	private static File pasta = new File("/home/yoshi/dados-teste/mil-txt");
	private static File[] arquivos = pasta.listFiles();
	private static int atual = -1;
	
	private IOpalaTextIndexer cliente;

	public ParallelIndexer(IOpalaTextIndexer cliente) {
		this.cliente = cliente;
	}

	@Override
	public void run() {
		File arquivo = null;
		while ( (arquivo = next()) != null) {
			try {

				MetaDocument metadoc = new MetaDocument();
				metadoc.setTitle(arquivo.getName());
				metadoc.setId(arquivo.getName());

				//Ler conteúdo do arquivo
				FileInputStream stream;
				stream = new FileInputStream(arquivo);
				InputStreamReader streamReader = new InputStreamReader(stream);
				BufferedReader reader = new BufferedReader(streamReader);

				StringBuffer sb = new StringBuffer();
				String line;
				while ((line = reader.readLine()) != null) {
					sb.append(line);
				}

				// Indexar documento
				MetaDocumentWrapper doc = new MetaDocumentWrapper(metadoc);
				Text conteudo = new Text(sb.toString());
						
				IntWritable code = cliente.addText(doc, conteudo);

				System.out.println(code);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private synchronized File next() {
		atual++;
		if (atual < arquivos.length) {
			return arquivos[atual];
		}else{
			return null; 
		}
	}

}