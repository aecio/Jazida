package br.edu.ifpi.jazida.util;

import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class WritableUtilsTest {

	@Test
	public void deveriaConverterUmMapWritableTextParaUmMapString() {
		// Dado
		MapWritable mapWritable = new MapWritable();
		mapWritable.put(new Text("chave"), new Text("valor"));
		// Quando
		Map<String, String> map = WritableUtils.convertMapWritableToMap(mapWritable);
		// Então
		final String valor = (String) map.get("chave");
		assertThat(valor, is(equalTo("valor")));
	}

}
