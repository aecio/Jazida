package br.edu.ifpi.jazida.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.jazida.util.UtilForTest;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ParallelTextSearchTest {
	
	private static final String SAMPLE_DATA_FOLDER = "./sample-data/texts";
	private static DataNode datanode;
	
	public static void main(String[] args) throws Exception {
		new ParallelTextSearchTest().deveriaEncontarUmDocumentoQueJaFoiIndexado();
	}
	
	@BeforeClass
	public static void setUpTest() throws Exception {
		FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue()));
		assertTrue(UtilForTest.indexTextDirOrFile(new File(SAMPLE_DATA_FOLDER)));
		datanode = new DataNode();
		datanode.start(false);
		
	}
	
	@AfterClass
	public static void tearDownTest() throws InterruptedException {
		datanode.stop();
	}
	

	@Test
	public final void deveriaEncontarUmDocumentoQueJaFoiIndexado() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "alice");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.AUTHOR.getValue());
		returnedFields.add(Metadata.TITLE.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult resultado = searcher.search(fields, returnedFields, 1, 10, Metadata.ID.getValue(), false);
		searcher.close();
		
		//então
		assertThat(resultado.getCodigo(), is(equalTo(ReturnMessage.SUCCESS)));
		assertThat(resultado.getItems().size(), is(equalTo(1)));
		assertThat(resultado.getItems().iterator().next().getField(Metadata.AUTHOR.getValue()), is(notNullValue()));
		assertThat(resultado.getItems().iterator().next().getField(Metadata.TITLE.getValue()), is(notNullValue()));
		assertThat(resultado.getItems().iterator().next().getField(Metadata.KEYWORDS.getValue()), is(nullValue()));
	}
	
	@Test
	public final void naoDeveriaEncontarUmTermoQueNaoFoiIndexado() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "adfhadsfasdfglhasdfjasdf3431383h123h12ih1");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.AUTHOR.getValue());
		returnedFields.add(Metadata.TITLE.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult resultado = searcher.search(fields, returnedFields, 1, 10, Metadata.ID.getValue(), false);
		searcher.close();
		
		//então
		assertThat(resultado.getCodigo(), is(equalTo(ReturnMessage.EMPTY_SEARCHER)));
		assertThat(resultado.getItems().size(), is(equalTo(0)));
	}
	
	
	/**
	 * Testa a busca com fields vazio e espera INVALID_QUERY
	 */
	@Test
	public final void deveriaRetornarInvalidQueryQuandoFieldsEhUmaListaVazia() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		List<String> returnedFields = new ArrayList<String>();
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult resultado = searcher.search(fields, returnedFields, 1, 10, null, false);
		searcher.close();
		
		//então
		assertThat(resultado.getCodigo(), is(equalTo(ReturnMessage.INVALID_QUERY)));
	}
	
	
	/**
	 * Testa a busca com fields null e espera INVALID_QUERY
	 */
	@Test
	public void deveriaRetornarInvalidQueryQuandoFieldsEhNull() throws Exception {
		//dado
		List<String> returnedFields = new ArrayList<String>();
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult resultado = searcher.search(null, returnedFields, 1, 10, null, false);
		searcher.close();
		
		//então
		assertThat(resultado.getCodigo(), is(equalTo(ReturnMessage.INVALID_QUERY)));
	}
	
	
	/**
	 * Testa o envio de returnedFields vazio e espera SUCCESS
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws Exception 
	 */
	@Test
	public void deveriaRetornarSuccessMesmoQuandoReturnedFieldsEstaVazio() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "alice");
		
		List<String> returnedFields = new ArrayList<String>();
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();

		//quando
		SearchResult searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
		searcher.close();
		
		//entao
		assertEquals(ReturnMessage.SUCCESS, searchResult.getCodigo());
	}
	
	
	/**
	 * Testa o envio de returnedFields null e espera SUCCESS
	 */
	@Test
	public void deveriaRetornarSuccessMesmoPassandoReturnedFieldsNull() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "alice");
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();

		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, null, false);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(equalTo(ReturnMessage.SUCCESS)));
	}
	
	/**
	 * Testa a busca com um returnedField não-existente e espera NULL
	 */
	@Test
	public void deveriaRetornarSuccessQuandoEhSolicitadoUmCampoNaoExistenteNoIndice() 
	throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "alice");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add("diaEmQueOAutorNasceu");
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();

		//quando
		SearchResult searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(equalTo(ReturnMessage.SUCCESS)));
		assertThat(searchResult.getItem(0).getField("diaEmQueOAutorNasceu"), is(nullValue()));
	}

	/**
	 * Verifica se um documento marcado como duplicado tem o mesmo
	 * score que o seu anterior.
	 * Deve encontrar os documentos: "Americana_M.txt" e "Americana_M2.txt"
	 * 
	 */
	@Test
	public void deveriaMarcarOSegundoDocumentoRepetidoComoDuplicated() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Rodrigo Barbosa Reis");
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, null, false);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(equalTo(ReturnMessage.SUCCESS)));
		assertThat(searchResult.getItem(0).isDuplicated(), is(false));
		assertThat(searchResult.getItem(1).isDuplicated(), is(true));
		assertEquals(searchResult.getItem(0).getScore(), searchResult.getItem(1).getScore());
	}
	
	
	/**
	 * Testa a busca por campo inexistente e espera nenhum resultado
	 */
	@Test
	public void deveriaRetornarEmptySearcherQuandoBuscaEmCampoNaoExistente() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put("campoQueNaoExisteNoIndice", "Um valor qualquer para ser buscado");
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, null, false);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(equalTo(ReturnMessage.EMPTY_SEARCHER)));
		assertThat(searchResult.getItems().size(), is(0) );
	}
	
	/**
	 * Testa busca com sort null e espera que os resultados estejam ordenados por score
	 */
	@Test
	public void deveriaOrdenarResultadosPorRelevanciaQuandoSortOnEhNull() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, null, false);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(equalTo(ReturnMessage.SUCCESS)));
		assertTrue(theThreeFirstsAreOrderedByScore(searchResult));
	}

	/**
	 * Testa busca com sort em campo que não existe e espera UNSORTABLE_FIELD e que os resultados estejam ordenados por score
	 */
	@Test
	public void deveriaRetornarUnsortableFieldQuandoOrdenacaoEhFeitaEmCampoInexistente()  throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, "CampoInexistenteNoIndice", false);
		searcher.close();
		
		for(ResultItem item: searchResult.getItems()) {
			System.out.println(item.getScore());
		}

		//entao
		assertThat(theThreeFirstsAreOrderedByScore(searchResult), is(true));
		assertThat(searchResult.getCodigo(), is(ReturnMessage.UNSORTABLE_FIELD));
	}
	
	/**
	 * Testa busca com sort em campo que existe mas não é ordenável e espera UNSORTABLE_FIELD e que os resultados estejam ordenados por score
	 */
	@Test
	public void deveriaDevolverUnsortableFieldQuandoSortOnEhUmCampoQueNaoPodeSerOrdenado() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, Metadata.AUTHOR.getValue(), false);
		searcher.close();
		
		//entao
		assertThat(theThreeFirstsAreOrderedByScore(searchResult), is(true));
		assertThat(searchResult.getCodigo(), is(ReturnMessage.UNSORTABLE_FIELD));
	}

	/**
	 * Testa busca com sort em campo que existe e espera SUCCESS e que os resultados estejam ordenados pelo campo informado
	 */
	@Test
	public void deveriaOrdenarPeloCampoEspecificadoEmSortOn() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, Metadata.AUTHOR.getValue());
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theThreeFirstsAreOrderedByID(searchResult), is(true));
	}

	/**
	 * Teste de busca informando o parâmetro reverse como true e espera SUCCESS e que os 
	 * resultados estejam ordenados inversamente por score
	 */
	@Test
	public void deveriaOrdenarPorOrdemDecrescenteQuandoReverseEhTrue() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, Metadata.ID.getValue(), true);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theThreeFirstsAreOrderedByID(searchResult), is(false));
		fail("Rever esse teste. Verificar ordem decrescente?");
	}
	/**
	 * Testa se busca retorna os resultados ordenados em ordem crescente quando o parâmetro
	 * 'reverse' é falso. 
	 */
	@Test
	public void deveriaOrdenarEmOrdemCrescenteQuandoReverseEhFalse() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, 10, Metadata.ID.getValue(), false);
		searcher.close();
		
		for(ResultItem item:searchResult.getItems())
			System.out.println(item.getField(Metadata.ID.getValue()));
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theThreeFirstsAreOrderedByID(searchResult), is(true));
	}

	

	/**
	 * Teste de busca com batchStart negativo e espera SUCCESS
	 */
	@Test
	public void deveriaDesconsiderarBatchStartComValorNegativo() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, -1, 10, null);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theThreeFirstsAreOrderedByScore(searchResult), is(true));
	}


	/**
	 * Teste de busca com batchStart maior que o da busca anterior, espera SUCCESS e que o número de resultados seja menor em 1
	 */
	@Test
	public void deveriaRetornarAQuantidadeDeHistCorretaQuandoBatchStartAumenta() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult1 = searcher.search(fields, null, 1, 10, null);
		SearchResult searchResult2 = searcher.search(fields, null, 2, 10, null);
		searcher.close();
		
		int numDoc1 = searchResult1.getItems().size();
		int numDoc2 = searchResult2.getItems().size();
		
		//entao
		assertThat(searchResult1.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(searchResult2.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theThreeFirstsAreOrderedByScore(searchResult1), is(true));
		assertThat(theThreeFirstsAreOrderedByScore(searchResult2), is(true));
		
		assertThat(numDoc1, is( numDoc2+1 ));
	}
	
	/**
	 * Teste de busca com batchSize menor que o número de resultados da busca anterior, espera SUCCESS e que o número de resultados seja menor em 1
	 */
	@Test
	public void deveriaRetornarMenosItensQuandoBatchSizeEhMenorUmaUnidade() throws Exception {
		
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult1 = searcher.search(fields, null, 1, 10, null);
		int numDoc1 = searchResult1.getItems().size();
		
		SearchResult searchResult2 = searcher.search(fields, null, 1, numDoc1-1, null);
		int numDoc2 = searchResult2.getItems().size();
		
		searcher.close();
		
		//entao
		assertThat(searchResult1.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(searchResult2.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theThreeFirstsAreOrderedByScore(searchResult1), is(true));
		assertThat(theThreeFirstsAreOrderedByScore(searchResult2), is(true));
		assertThat(numDoc1-1, is( numDoc2 ));
	}
	
	/**
	 * Teste de busca com batchStart zero e espera SUCCESS
	 */
	@Test
	public void deveriaRetornarSuccessEIgnorarBatchStartQuandoEhIgualAZero() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 0, 10, null);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theThreeFirstsAreOrderedByScore(searchResult), is(true));
	}

	/**
	 * Teste de busca com batchStart maior que o número de hits e espera nenhum resultado
	 */
	@Test
	public void deveriaRetornarEmptySearcherCasoBatchStartSejaMaiorONumeroDeHits() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		searcher.close();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1000, 10, null, false);
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.EMPTY_SEARCHER));
	}

	/**
	 * Teste de busca com batchSize negativo e espera SUCCESS
	 */
	@Test
	public void deveruaRetornarSuccessMesmoComBatchSizeComValorNegativo() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), "Aécio");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, null, 1, -1, null, false);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(searchResult.getItems(), is(not(nullValue())));
		
	}

	/**
	 * Testa busca com query inválida e espera INVALID_QUERY
	 */
	@Test
	public void deveriaRetornarInvalidQueryQuandoUmCampoInvalidoEhPassadoNaBusca() throws Exception {
		//dado
		Map<String, String> fields = new HashMap<String, String>();
		fields.put("[author", "Algum valor");
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		returnedFields.add(Metadata.ID.getValue());
		
		ParallelTextSearcherClient searcher = new ParallelTextSearcherClient();
		
		//quando
		SearchResult searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
		searcher.close();
		
		//entao
		assertThat(searchResult.getCodigo(), is(ReturnMessage.INVALID_QUERY));
	}
	
	
	/**
	 * Método auxiliar da classe de testes que retorna se os três primeiros
	 * resultados estão ordenados por relevância (score)
	 * @return 
	 * 
	 * @return true se estiverem ordenados por relevância
	 */
	private boolean theThreeFirstsAreOrderedByScore(SearchResult searchResult) {
		return Float.parseFloat(searchResult.getItem(0).getScore()) 
				>= Float.parseFloat(searchResult.getItem(1).getScore())
					&& 
				Float.parseFloat(searchResult.getItem(1).getScore()) 
				>= Float.parseFloat(searchResult.getItem(2).getScore());
	}
	
	/**
	 * Método auxiliar da classe de testes que retorna se os três primeiros
	 * resultados estão ordenados pelo campo "Metadata.ID"
	 * 
	 * @return true se estiverem ordenados por "Metadata.ID"
	 */
	private boolean theThreeFirstsAreOrderedByID(SearchResult searchResult) {
		return searchResult.getItem(0).getField(Metadata.ID.getValue()).compareTo(
				searchResult.getItem(1).getField(Metadata.ID.getValue())) < 0
				&& searchResult.getItem(1).getField(Metadata.ID.getValue()).compareTo(
						searchResult.getItem(2).getField(Metadata.ID.getValue())) < 0;
	}
}