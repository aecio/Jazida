package br.edu.ifpi.jazida.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class JazidaConfTest {

	@Test
	public void deveriaRetornarAlgumHostDoArquivoDeConfiguracao() {
		assertNotNull(ZkConf.ZOOKEEPER_SERVERS);
	}

}
