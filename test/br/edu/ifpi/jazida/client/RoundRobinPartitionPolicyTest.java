package br.edu.ifpi.jazida.client;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class RoundRobinPartitionPolicyTest {

	@Test
	public void testNextNode() {
		List<String> nodes = new ArrayList<String>();
		nodes.add("mario");
		nodes.add("luigi");
		nodes.add("toad");
		RoundRobinPartitionPolicy policy = new RoundRobinPartitionPolicy(nodes);
		
		assertTrue(policy.nextNode().equals("mario"));
		assertTrue(policy.nextNode().equals("luigi"));
		assertTrue(policy.nextNode().equals("toad"));
		assertTrue(policy.nextNode().equals("mario"));
		assertTrue(policy.nextNode().equals("luigi"));
		assertTrue(policy.nextNode().equals("toad"));
		
	}

}
