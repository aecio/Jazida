package br.edu.ifpi.jazida.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import br.edu.ifpi.opala.utils.MetaDocument;

public class JazidaClientTest {
	
	private JazidaClient jazidaClient;

	@Before
	public void setUp() throws KeeperException, InterruptedException, IOException {
		jazidaClient = new JazidaClient();
	}

	private static File pasta = new File("/home/aecio/workspace/lapesi/dez-mil-txt");
	private static File[] arquivos = pasta.listFiles();
	private static int atual = -1;
	
	@Test
	public void testAddText() {
	
		File arquivo = nextDocument();
		while (arquivo != null) {
			try {
				MetaDocument metadoc = new MetaDocument();
				metadoc.setTitle(arquivo.getName());
				metadoc.setId(arquivo.getName());
				
				// Ler conteúdo do arquivo
				FileInputStream stream;
				stream = new FileInputStream(arquivo);
				InputStreamReader streamReader = new InputStreamReader(
						stream);
				BufferedReader reader = new BufferedReader(streamReader);
				
				StringBuffer stringBuffer = new StringBuffer();
				String line;
				while ((line = reader.readLine()) != null) {
					stringBuffer.append(line);
				}
				reader.close();
				streamReader.close();
				stream.close();
				
				// Indexar documento
				int code = jazidaClient.addText(metadoc, stringBuffer.toString());
				
				System.out.println("ResultCode: "+code);
				
			} catch (Exception e) {
				System.out.println("Erro ao indexar arquivo...");
				e.printStackTrace();
				System.exit(1);
			}
			arquivo = nextDocument();
		}
		
	}
	
	private synchronized static File nextDocument() {
		atual++;
		if (atual < arquivos.length) {
			System.out.println("Arquivo: "+atual+"/"+arquivos.length);
			return arquivos[atual];
		} else {
			return null;
		}
	}
}
