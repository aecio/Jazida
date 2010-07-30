package br.edu.ifpi.jazida.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class WritableUtils {

	/**
	 * Conversão de {@link MapWritable}&lt;Text, Text&gt; para um {@link Map}&lt;String,String&gt;.
	 * 
	 * @param metaDocumentMap Um mapa do tipo MapWritable&lt;Text, Text&gt;.
	 * @return Um mapa do tipo Map&lt;String,String&gt equivalente ao recebido.
	 */
	public static Map<String, String> convertMapWritableToMap(MapWritable metaDocumentMap) {
		Map<String, String> novoMapa = new HashMap<String, String>();
		Set<Entry<Writable, Writable>> keys = metaDocumentMap.entrySet();
		for (Entry<Writable, Writable> each : keys) {
			novoMapa.put(each.getKey().toString(), each.getValue().toString());
		}
		return novoMapa;
	}
}
