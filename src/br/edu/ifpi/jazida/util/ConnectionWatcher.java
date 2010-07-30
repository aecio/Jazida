package br.edu.ifpi.jazida.util;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Implementa funcionalidades básicas de conexão com o Zookeeper. Classes que
 * necessitem se comunicar com o Zookeeper devem extender esta classe.
 * 
 * @author Aécio Santos
 * 
 */
public class ConnectionWatcher implements Watcher {

	private static final int SESSION_TIMEOUT = 2000;
	private static final Logger LOG = Logger.getLogger(ConnectionWatcher.class);
	protected ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	protected void connect(String hosts) throws IOException, InterruptedException {
		LOG.info("\n-----------------------------------");
		LOG.info("Conectando-se ao Zookeeper...");
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
			LOG.info("Conectado.");
		}
	}

	public void disconnect() throws InterruptedException {
		zk.close();
	}

}
