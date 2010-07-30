package br.edu.ifpi.jazida.client;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import br.edu.ifpi.jazida.node.NodeStatus;

public class RoundRobinPartitionPolicyTest {

	@Test
	public void deveriaRetornarOsNosCircularmente() {
		//Dado
		List<NodeStatus> nodes = new ArrayList<NodeStatus>();
		NodeStatus node1 = new NodeStatus();
		NodeStatus node2 = new NodeStatus();
		NodeStatus node3 = new NodeStatus();
		
		nodes.add(node1);
		nodes.add(node2);
		nodes.add(node3);

		//Quando
		RoundRobinPartitionPolicy policy = new RoundRobinPartitionPolicy(nodes);
		
		//Então
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node2));
		assertTrue(policy.nextNode().equals(node3));
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node2));
		assertTrue(policy.nextNode().equals(node3));
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node2));
		assertTrue(policy.nextNode().equals(node3));
		
	}
	
	@Test
	public void deveriaRetornarSempreOMesmoNo() {
		//Dado
		List<NodeStatus> nodes = new ArrayList<NodeStatus>();
		NodeStatus node1 = new NodeStatus();
		nodes.add(node1);
		
		//Quando
		RoundRobinPartitionPolicy policy = new RoundRobinPartitionPolicy(nodes);
		
		//Então
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node1));
	}
	

}
