package zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class HelloZooKepper {
	public static void main(String[] args) throws IOException {
		
		// comma separated host:port pairs.
		String connectString = "localhost:2181";
		
		// this is the amount of time ZooKeeper waits without
		// getting a heartbeat from the client before declaring the session
		// as dead.
		int sessionTimeout = 2000;
		
		// a watcher object which will be notified of state changes, 
		// may also be notified for node events.
		Watcher watcher = null;
		
		String zPath = "/";
		List<String> zooChildren = new ArrayList<>();
		
		
		// tries to connect to the ZooKeeper server and
		// returns a handle to it.
		// the handle returned by the constructor represents
		// our connection/live session of ZooKeeper.
		ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, watcher);
		if (zk != null) {
			try {
				
				// Return the list of the children of the node of the given path.
				zooChildren = zk.getChildren(zPath, false);
				for (String child: zooChildren) {
					System.out.println(child);
				}
			}
			catch(KeeperException e) {
				e.printStackTrace();
			}
			catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
