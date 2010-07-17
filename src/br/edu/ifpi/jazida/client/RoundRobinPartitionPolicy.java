package br.edu.ifpi.jazida.client;

import java.util.List;

import br.edu.ifpi.jazida.node.NodeStatus;

public class RoundRobinPartitionPolicy implements PartitionPolicy<NodeStatus> {

	private List<NodeStatus> nodes;
	private int currentNode = 0;
	
	public RoundRobinPartitionPolicy(List<NodeStatus> liveNodes){
		this.nodes = liveNodes;
	}
	
	@Override
	public NodeStatus nextNode() {
		NodeStatus node = nodes.get(currentNode);
		currentNode++;
		if( currentNode >= nodes.size() ){
			currentNode = 0;
		}
		return node;
	}

}
