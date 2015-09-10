package dataWatcher;

import java.io.IOException;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class DataUpdater implements Watcher, Runnable {
	private String connectString = "localhost:2181";
	private int sessionTimeout = 2000;
	private String zooDataPath = "/MyConfig";
	Watcher watcher = this;
	ZooKeeper zk;
	
	public DataUpdater() {
		try {
			// try to connect to the ZooKeeper instance that is running on localhost.
			// if the conncetion is successful, we check whether the znode
			// path /MyConfig exists
			zk = new ZooKeeper(connectString, sessionTimeout, watcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("\n********** From DataUpdater Class  **********");
		System.out.printf("\nEvent State %s: ", event.getClass().getSimpleName());
		System.out.printf("\nEvent State %s: ", event.getState().name());
		System.out.printf("\nEvent Type %s: ", event.getType());
		System.out.printf("\nEvent Path %s: ", event.getPath());
		System.out.printf("\nEvent Received %s: ", event.toString());
		System.out.println();
	}

	@Override
	public void run() {
		while (true) {
			String uuid = UUID.randomUUID().toString();
			byte[] zoo_data = uuid.getBytes();
			try {
				zk.setData(zooDataPath, zoo_data, -1);
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		DataUpdater dataUpdater = new DataUpdater();
		dataUpdater.run();
	}
}
