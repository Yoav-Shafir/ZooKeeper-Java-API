package clusterMonitor;

import java.io.IOException;
import java.util.List;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

// simulating a cloud controller node which runs a ZooKeeper watcher process
// which keeps a watch on the path "/Members".

public class ClusterMonitor implements Runnable {
	
	private static String membershipRoot = "/Members";
	private final Watcher connectionWatcher;
	private final Watcher childrenWatcher;
	private ZooKeeper zk;
	boolean alive = true;
	
	public ClusterMonitor(String hostPort) throws IOException, InterruptedException, KeeperException {
		connectionWatcher = new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				if (event.getType() == Watcher.Event.EventType.None && event.getState() == Watcher.Event.KeeperState.SyncConnected) {
					System.out.printf("\nEvent Received: %s ", event.toString());
				}
				
			}
		};
		
		childrenWatcher = new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.out.printf("\nEvent Received: %s ", event.toString());
				if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
					try {
						// get current list of child znode,
						// reset the watch
						List<String> children = zk.getChildren(membershipRoot, this);
						wall("!!!Cluster Membership Change!!!");
						wall("Members: " + children);
					} catch (KeeperException e) {
						throw new RuntimeException(e);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						alive = false;
						throw new RuntimeException();
					}
				}
			}
		};
		// create a ZooKeeper client object with the 'connectionWatcher'.
		zk = new ZooKeeper(hostPort, 2000, connectionWatcher);
		
		// Ensure the parent znode exists
		if (zk.exists(membershipRoot, false) == null) {
			zk.create(membershipRoot, "ClusterMonotorRoot".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		// Return the list of the children of the node of the given path. 
		// a watch will be left on the node with the given path. The watch will be triggered by a successful operation 
		// that deletes the node of the given path or creates/delete a child under the node.
		List<String> children = zk.getChildren(membershipRoot, childrenWatcher);
		System.err.println("\nMembers: " + children);
	}
	
	public synchronized void close() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void wall (String message) {
		System.out.printf("\nMESSAGE: %s ", message);
	}
	
	@Override
	public void run() {
		try {
			synchronized (this) {
				while (alive) {
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
		new ClusterMonitor(hostPort).run();
	}

}
