package br.edu.ifpi.jazida.client;

import static br.edu.ifpi.jazida.util.FileUtilsForTest.conteudoDoArquivo;
import static br.edu.ifpi.jazida.util.UtilForTest.novoMetaDocumento;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

/**
 * Teste para {@link TextIndexerClient}. Para funcionar, os Serviço do Zookeeper
 * deve estar iniciado.
 * 
 * @author Aécio Solano Rodrigues Santos
 */
public class TextIndexerClientTest {

	private static final String SAMPLE_DATA_ALICE_TXT = "./sample-data/texts/alice.txt";
	private static DataNode datanode;
	private TextIndexerClient textIndexerClient;

	@BeforeClass
	public static void setUpTest() throws Exception {
		datanode = new DataNode();
		datanode.start(false);
	}
	
	@AfterClass
	public static void tearDownTest() throws InterruptedException {
		datanode.stop();
	}

	@Before
	public void setUp() {
		//		
		//Apagar o índice para cada teste
		//
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue())));
	}

	@Test
	public void deveriaAdicionarUmDocumentoDeTextoNoIndiceDistribuido()
	throws KeeperException, InterruptedException, IOException {
		// Dado
		textIndexerClient = new TextIndexerClient();
		File aliceTxt = new File(SAMPLE_DATA_ALICE_TXT);
		MetaDocument metaDocument = novoMetaDocumento("Alice's Adventures in Wonderland", "Lewis Carroll", aliceTxt);
		String texto = conteudoDoArquivo(aliceTxt);

		// Quando
		Integer code = textIndexerClient.addText(metaDocument, texto);

		// Então
		assertThat(code, is(ReturnMessage.SUCCESS.getCode()));
	}

}
