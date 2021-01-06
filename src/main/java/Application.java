import cluster.management.LeaderElection;
import cluster.management.ServiceRegistry;
import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class Application implements Watcher {
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final int DEFAULT_PORT = 8080;

	private ZooKeeper zooKeeper;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		int currentServicePort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
		Application application = new Application();
		ZooKeeper zooKeeper = application.connectToZookeeper();

		ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);
		OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServicePort);

		LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);

		leaderElection.volunteerForLeadership();
		leaderElection.reelectLeader();

		application.run();
		application.close();
		System.out.println("Exiting the application");
	}

	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	public ZooKeeper connectToZookeeper() throws IOException {
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
		return zooKeeper;
	}

	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
			case None:
				if (event.getState() == KeeperState.SyncConnected) {
					System.out.println("Connected to ZooKeeper");
				} else {
					System.out.println("Disconnected from ZooKeeper event");
					synchronized (zooKeeper) {
						zooKeeper.notifyAll();
					}
				}
				break;
		}
	}
}
