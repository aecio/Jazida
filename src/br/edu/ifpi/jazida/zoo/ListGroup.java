package br.edu.ifpi.jazida.zoo;

import java.util.List;

import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.util.Configuration;
import br.edu.ifpi.jazida.util.ConnectionWatcher;

public class ListGroup extends ConnectionWatcher {
	public void list(String groupName) throws KeeperException, InterruptedException {
		String path = Configuration.DATANODES_PATH;
		try{
			List<String> datanodes = zk.getChildren(path, false);
			
			if(datanodes.isEmpty()){
				System.out.println("Nenhum mebro no grupo "+groupName+" encontrado");
				System.exit(1);
			}
			for (String child : datanodes) {
				System.out.println(child);
			}
		}catch (KeeperException.NoNodeException e) {
			System.out.printf("O grupo %s não existe.\n", groupName);
			System.exit(1);
		}
	}
	
	public static void main(String[] args) throws Exception {
		ListGroup listGroup = new ListGroup();
		listGroup.connect("localhost");
		listGroup.list("jazida");
		listGroup.close();
	}
}
