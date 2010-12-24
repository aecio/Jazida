package br.edu.ifpi.jazida.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import br.edu.ifpi.jazida.node.NodeStatus;

/**
 * Implementa uma estratégia simples de distribuição de forma circular.
 * 
 * @author Aécio Santos
 * 
 */
public class RoundRobinPartitionPolicy implements PartitionPolicy<NodeStatus> {

	private List<NodeStatus> nodes = new ArrayList<NodeStatus>();
	private int currentNode = 0;

	@Override
	public NodeStatus nextNode() {
		NodeStatus node = nodes.get(currentNode);
		currentNode++;
		if (currentNode >= nodes.size()) {
			currentNode = 0;
		}
		return node;
	}

	@Override
	public void addNode(NodeStatus... liveNodes) {
		nodes.addAll(Arrays.asList(liveNodes));
	}
	
	@Override
	public void removeNode(NodeStatus... deadNodes) {
		nodes.removeAll(Arrays.asList(deadNodes));
	}

}
