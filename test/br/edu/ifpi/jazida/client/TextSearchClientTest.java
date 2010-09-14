package br.edu.ifpi.jazida.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.jazida.util.UtilForTest;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextSearchClientTest {
	
	private static final String SAMPLE_DATA_FOLDER = "./sample-data/texts";
	private static DataNode datanode;
	
	@BeforeClass
	public static void setUpTest() throws Exception {
		datanode = new DataNode();
		datanode.start(false);
		
		FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue()));
		assertTrue(UtilForTest.indexTextDirOrFile(new File(SAMPLE_DATA_FOLDER)));
	}
	
	@AfterClass
	public static void tearDownTest() throws InterruptedException {
		datanode.stop();
		FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue()));
	}
	

	@Test
	public final void deveriaEncontarUmDocumentoQueJaFoiIndexado() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Alice");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.AUTHOR.getValue());
		returnedFields.add(Metadata.TITLE.getValue());
		
		TextSearchClient searcher = new TextSearchClient();
		
		//quando
		SearchResult resultado = searcher.search(fields, 
												returnedFields,
												1,
												10,
												Metadata.ID.getValue(),
												false);

		//então
		assertThat(resultado.getCodigo(), is(equalTo(ReturnMessage.SUCCESS)));
		assertThat(resultado.getItems().size(), is(equalTo(1)));
		assertThat(resultado.getItems().iterator().next().getField(Metadata.AUTHOR.getValue()), is(notNullValue()));
		assertThat(resultado.getItems().iterator().next().getField(Metadata.TITLE.getValue()), is(notNullValue()));
	}

	@Test
	public final void deveriaRetorarOMetodoSearch() {
		//dado
		Method searchMethod = null;
		//quando
		searchMethod = TextSearchClient.getSearchMethod();
		//então
		assertThat(searchMethod, is(instanceOf(Method.class)));
	}
	
}
