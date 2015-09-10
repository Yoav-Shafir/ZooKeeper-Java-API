package dataWatcher;

import org.apache.zookeeper.KeeperException;

public class Program {
	public static void main(String[] args) throws KeeperException, InterruptedException {
		DataWatcher dataWatcher = new DataWatcher();
		dataWatcher.printData();
		dataWatcher.run();
	}
}
