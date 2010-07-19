package br.edu.ifpi.jazida.zoo;

import java.net.InetAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import br.edu.ifpi.jazida.util.ConnectionWatcher;

public class JoinGroup extends ConnectionWatcher {
	
	public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
		String path = "/"+groupName+"/"+memberName;
		String createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		System.out.println("Nó entrou no grupo: "+createdPath);
	}
	
	public static void main(String[] args) throws Exception {
		JoinGroup jg = new JoinGroup();
		jg.connect("localhost");
		jg.join("jazida", InetAddress.getLocalHost().getHostName());
		
		Thread.sleep(Long.MAX_VALUE);
	}

}
