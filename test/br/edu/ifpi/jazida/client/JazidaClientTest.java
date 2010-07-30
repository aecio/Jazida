package br.edu.ifpi.jazida.client;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.opala.utils.MetaDocument;

/**
 * Teste para {@link JazidaClient}. Para funcionar, os Serviço do Zookeeper deve
 * estar iniciado.
 * 
 * @author Aécio Solano Rodrigues Santos
 */
public class JazidaClientTest {

	private static DataNode datanode;
	private JazidaClient jazidaClient;

	@BeforeClass
	public static void setUpTest() throws KeeperException,
			InterruptedException, IOException {
		datanode = new DataNode();
		datanode.start(false);
	}

	@Test
	public void deveriaAdicionarUmDocumentoDeTextoNoIndiceDistribuido() 
	throws KeeperException, InterruptedException, IOException {

		//Dado
		jazidaClient = new JazidaClient();
		File arquivo = new File("./sample-data/texts/alice.txt");

		MetaDocument metadoc = new MetaDocument();
		metadoc.setAuthor("Lewis Carroll");
		metadoc.setTitle("Alice's Adventures in Wonderland");
		metadoc.setId(arquivo.getName());

		// Ler conteúdo do arquivo
		InputStreamReader isr = new InputStreamReader(new FileInputStream(arquivo));
		BufferedReader reader = new BufferedReader(isr);
		
		StringBuffer stringBuffer = new StringBuffer();
		String line = null;
		while ((line = reader.readLine()) != null) {
			stringBuffer.append(line);
		}
		reader.close();
		isr.close();

		//Quando
		Integer code = jazidaClient.addText(metadoc, stringBuffer.toString());
		
		//Então
		assertNotNull(code);
	}
}
