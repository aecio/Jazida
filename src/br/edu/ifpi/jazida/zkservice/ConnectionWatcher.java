package br.edu.ifpi.jazida.zkservice;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import br.edu.ifpi.jazida.util.ZkConf;

/**
 * Implementa funcionalidades básicas de conexão com o Zookeeper. Classes que
 * necessitem se comunicar com o Zookeeper devem extender esta classe.
 * 
 * @author Aécio Santos
 * 
 */
public class ConnectionWatcher implements Watcher {

	private static final Logger LOG = Logger.getLogger(ConnectionWatcher.class);
	protected ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public ConnectionWatcher() {
	}
	
	public ConnectionWatcher(ZooKeeper zk) {
		this.zk = zk;
	}

	protected void connect(String hosts) throws IOException, InterruptedException {
		LOG.info("\n-----------------------------------");
		LOG.info("Conectando-se ao Zookeeper...");
		zk = new ZooKeeper(hosts, ZkConf.ZOOKEEPER_TIMEOUT, this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
			LOG.info("Conectado ao Zookeeper.");
		}
	}

	public void disconnect() throws InterruptedException {
		LOG.info("Desconectando-se ao Zookeeper...");
		zk.close();
		//
		// TODO: Esperar até que o zookeeper client desconecte.
		// Verificar uma melhor forma de fazer isso. Talvez recebendo 
		// notificação de conexão expirada (KeeperState.Expired)?
		//
	}

}
