package clusterMonitor;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

// simulating a production server which runs a zookeeper client as a daemon process.
// this process connects to the ZooKeeper server and creates a znode
// with its process id as its name under a predefined path in the ZooKeeper namespace,
// which in this example is "/Members".

public class ClusterClient implements Watcher, Runnable {
	
	private static String membershipRoot = "/Members";
	ZooKeeper zk;
	
	public ClusterClient(String hostPort, Long pid) {
		String processId = pid.toString();
		try {
			// create a ZooKeeper client object,
			zk = new ZooKeeper(hostPort, 2000, this);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (zk != null) {
			try {
				// Create a node with the given path.
				zk.create(membershipRoot + "/" + processId, 
						processId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public synchronized void close() {
		try {
			// Close this client object.
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		System.out.printf("\nEvent Received: %s ", event.toString());
	}	
	
	@Override
	public void run() {
		try {
			synchronized (this) {
				while (true) {
					wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
		} finally {
			this.close();
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		if (args.length !=1) {
			System.err.println("Usage: ClusterMonitor <Host:Port>");
			System.exit(0);
		}
		String hostPort = args[0];
		// Get processId.
		String name = ManagementFactory.getRuntimeMXBean().getName();
		int index = name.indexOf("@");
		Long processId = Long.parseLong(name.substring(0, index));
		new ClusterClient(hostPort, processId).run();
	}
}
