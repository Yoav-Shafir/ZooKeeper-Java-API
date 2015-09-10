package dataWatcher;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class DataWatcher implements Watcher, Runnable {
	
	private String connectString = "localhost:2181";
	private int sessionTimeout = 2000;
	Watcher watcher = this;
	private String zooDataPath = "/MyConfig";
	byte[] zoo_data = null;
	ZooKeeper zk;
	
	public DataWatcher() {
		try {
			
			// try to connect to the ZooKeeper instance that is running on localhost.
			// if the conncetion is successful, we check whether the znode
			// path /MyConfig exists
			zk = new ZooKeeper(connectString, sessionTimeout, watcher);
			if (zk != null) {
				try {
					
					// if the znode is not present in the ZooKeeper namespace,
					// we try to create it as a persistent znode.
					if (zk.exists(zooDataPath, watcher) == null) {
						zk.create(zooDataPath, "".getBytes(), 
								ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		try {
			synchronized(this) {
				while(true) {
					System.out.println("\n---------- waiting... ----------");
					wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("\n********** From DataWatcher Class  **********");
		System.out.printf("Event State %s: ", event.getClass().getSimpleName());
		System.out.printf("\nEvent State %s: ", event.getState().name());
		System.out.printf("\nEvent Type %s: ", event.getType());
		System.out.printf("\nEvent Path %s: ", event.getPath());
		System.out.printf("\nEvent Received %s: ", event.toString());
		System.out.println();
		if (event.getType() == Event.EventType.NodeDataChanged) {
			try {
				printData();
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void printData() throws KeeperException, InterruptedException {
		zoo_data = zk.getData(zooDataPath, watcher, null);
		String zString = new String(zoo_data);
		System.out.printf("\nCurrent Data @ ZK Path %s: %s", zooDataPath, zString);
		System.out.println();
	}
	
}
