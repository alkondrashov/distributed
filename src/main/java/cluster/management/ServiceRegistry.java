package cluster.management;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ServiceRegistry implements Watcher {
	private static final String REGISTRY_ZNODE = "/service_registry";
	private final ZooKeeper zooKeeper;
	private String currentZNode = null;
	private List<String> allServiceAddresses;

	public ServiceRegistry(ZooKeeper zooKeeper) {
		this.zooKeeper = zooKeeper;
		createServiceRegistryZNode();
	}

	private void createServiceRegistryZNode() {
		try {
			if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
				zooKeeper.create(REGISTRY_ZNODE, new byte[] {}, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public synchronized List<String> getAllServiceAddresses()
		throws KeeperException, InterruptedException {
		if (allServiceAddresses == null) {
			updateAddresses();
		}
		return allServiceAddresses;
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		try {
			updateAddresses();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void registerForUpdates() {
		try {
			updateAddresses();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
		this.currentZNode = zooKeeper.create(
			REGISTRY_ZNODE + "/n",
			metadata.getBytes(),
			Ids.OPEN_ACL_UNSAFE,
			CreateMode.EPHEMERAL_SEQUENTIAL
		);
		System.out.println("Registered to service registry");
	}

	public void unregisterFromCluster() {
		try {
			if (currentZNode != null && zooKeeper.exists(currentZNode, false) != null) {
				zooKeeper.delete(currentZNode, -1);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public synchronized void updateAddresses() throws KeeperException, InterruptedException {
		List<String> workerZNodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
		List<String> addresses = new ArrayList<>(workerZNodes.size());

		for (String workerZNode : workerZNodes) {
			String workerZNodePath = REGISTRY_ZNODE + "/" + workerZNode;
			Stat stat = zooKeeper.exists(workerZNodePath, false);
			if (stat == null) {
				continue;
			}

			byte[] addressByte = zooKeeper.getData(workerZNodePath, false, stat);
			String address = new String(addressByte);
			addresses.add(address);
		}

		this.allServiceAddresses = Collections.unmodifiableList(addresses);
		System.out.println("The cluster addresses are : " + this.allServiceAddresses);
	}
}
