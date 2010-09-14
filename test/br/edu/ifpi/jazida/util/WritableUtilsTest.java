package br.edu.ifpi.jazida.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class WritableUtilsTest {

	@Test
	public void deveriaConverterUmMapWritableTextParaUmMapString() {
		// Dado
		MapWritable mapWritable = new MapWritable();
		mapWritable.put(new Text("chave"), new Text("valor"));
		// Quando
		Map<String, String> map = WritableUtils
				.convertMapWritableToMap(mapWritable);
		// Então
		final String valor = (String) map.get("chave");
		assertThat(valor, is(equalTo("valor")));
	}

	@Test
	public void deveriaConverterUmaListaDeTextParaListaDeStrings() {
		// dado
		List<Text> lista = new ArrayList<Text>();
		lista.add(new Text("texto1"));
		lista.add(new Text("texto2"));
		lista.add(new Text("texto3"));
		
		// quando
		List<String> novaLista = WritableUtils.convertTextListToStringList(lista);
		
		// então
		assertThat(novaLista, contains("texto1","texto2","texto3"));
	}

}
