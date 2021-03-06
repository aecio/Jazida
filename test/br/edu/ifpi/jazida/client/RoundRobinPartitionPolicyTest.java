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
		NodeStatus node1 = new NodeStatus("host1", "127.0.0.1", 16001, 15001, 14001, 13001);
		NodeStatus node2 = new NodeStatus("host2", "127.0.0.1", 16002, 15002, 14002, 13002);
		NodeStatus node3 = new NodeStatus("host3", "127.0.0.1", 16003, 15003, 14003, 13003);
		
		nodes.add(node1);
		nodes.add(node2);
		nodes.add(node3);

		//Quando
		RoundRobinPartitionPolicy policy = new RoundRobinPartitionPolicy();
		policy.addNode(nodes.toArray(new NodeStatus[nodes.size()]));
		
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
		NodeStatus node1 = new NodeStatus("host1", "127.0.0.1", 16000,15000, 14000, 13000);
		nodes.add(node1);
		
		//Quando
		RoundRobinPartitionPolicy policy = new RoundRobinPartitionPolicy();
		policy.addNode(nodes.toArray(new NodeStatus[nodes.size()]));
		
		//Então
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node1));
	}
	
}

