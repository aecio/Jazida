package br.edu.ifpi.jazida.client;

import java.util.List;

public class RoundRobinPartitionPolicy implements PartitionPolicy {

	private List<String> nodes;
	private int currentNode = 0;
	
	public RoundRobinPartitionPolicy(List<String> liveNodes){
		this.nodes = liveNodes;
	}
	
	@Override
	public String nextNode() {
		String node = nodes.get(currentNode);
		currentNode++;
		if( currentNode >= nodes.size() ){
			currentNode = 0;
		}
		return node;
	}

}
