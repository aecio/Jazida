package br.edu.ifpi.jazida.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import br.edu.ifpi.jazida.client.RoundRobinPartitionPolicyTest;
import br.edu.ifpi.jazida.node.NodeStatusTest;
import br.edu.ifpi.jazida.util.WritableUtilsTest;


@RunWith(Suite.class)
@Suite.SuiteClasses({
	RoundRobinPartitionPolicyTest.class,
	WritableUtilsTest.class,
	NodeStatusTest.class
})

/**
 * Esta classe executa todos os métodos de testes contidos nas classes de teste 
 * definida acima devendo ser executado como "JUnit Test"
 *  
 * @author Aécio Santos
 *
 */
public class UnitTests {
	
}