package cluster.management;

import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher {
	private static final String ELECTION_NAMESPACE = "/election";

	private ZooKeeper zooKeeper;
	private String currentZNodeName;
	private OnElectionCallback onElectionCallback;

	public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
		this.zooKeeper = zooKeeper;
		this.onElectionCallback = onElectionCallback;
	}

	public void reelectLeader() throws KeeperException, InterruptedException {
		Stat predecessorStat = null;
		String predecessorZNodeName = "";

		while (predecessorStat == null) {
			List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
			Collections.sort(children);
			String smallestChild = children.get(0);

			if (smallestChild.equals(currentZNodeName)) {
				System.out.println("I am the leader");
				onElectionCallback.onElectedToBeLeader();
				return;
			} else {
				System.out.println("I am not the leader, " + smallestChild + " is the leader");
				int predecessorIndex = Collections.binarySearch(children, currentZNodeName) - 1;
				predecessorZNodeName = children.get(predecessorIndex);
				predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZNodeName, this);
			}
		}

		onElectionCallback.onWorker();

		System.out.println("Watching znode " + predecessorZNodeName);
		System.out.println();
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
			case NodeDeleted:
				try {
					reelectLeader();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
		}
	}

	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		String zNodePrefix = ELECTION_NAMESPACE + "/c_";
		String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[] {}, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("znode name " + zNodeFullPath);
		currentZNodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
	}
}
