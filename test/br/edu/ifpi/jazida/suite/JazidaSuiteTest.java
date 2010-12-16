package br.edu.ifpi.jazida.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import br.edu.ifpi.jazida.client.ImageIndexerClientTest;
import br.edu.ifpi.jazida.client.ImageSearchClientTest;
import br.edu.ifpi.jazida.client.RoundRobinPartitionPolicyTest;
import br.edu.ifpi.jazida.client.TextIndexerClientTest;
import br.edu.ifpi.jazida.client.TextSearchClientTest;
import br.edu.ifpi.jazida.node.NodeStatusTest;
import br.edu.ifpi.jazida.node.ZookeeperServiceTest;
import br.edu.ifpi.jazida.util.JazidaConfTest;
import br.edu.ifpi.jazida.writable.WritableUtilsTest;


@RunWith(Suite.class)
@Suite.SuiteClasses({
	RoundRobinPartitionPolicyTest.class,
	NodeStatusTest.class,
	ZookeeperServiceTest.class,
	JazidaConfTest.class,
	WritableUtilsTest.class,
	TextIndexerClientTest.class,
	TextSearchClientTest.class,
	ImageIndexerClientTest.class,
	ImageSearchClientTest.class
})

/**
 * Esta classe executa todos os métodos de testes contidos nas classes de teste 
 * definida acima devendo ser executado como "JUnit Test"
 *  
 * @author Aécio Santos
 *
 */
public class JazidaSuiteTest {
	
}