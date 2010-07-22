package br.edu.ifpi.jazida.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import br.edu.ifpi.jazida.client.JazidaClientTest;
import br.edu.ifpi.jazida.client.RoundRobinPartitionPolicyTest;


@RunWith(Suite.class)
@Suite.SuiteClasses({
	JazidaClientTest.class,
	RoundRobinPartitionPolicyTest.class	
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