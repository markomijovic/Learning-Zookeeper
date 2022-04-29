import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

  private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
  private static final int SESSION_TIMEOUT = 3000;
  private static final String ELECTION_NAMESPACE = "/election";
  private ZooKeeper zooKeeper;
  private String currentZnodeName;
  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    LeaderElection leaderElection = new LeaderElection();
    leaderElection.connectToZookeeper();
    leaderElection.volunteerForLeadership();
    leaderElection.electLeader();
    leaderElection.run();
    leaderElection.close();
    System.out.println("DC-ed from zookeeper, exiting");
  }
  public void volunteerForLeadership() throws InterruptedException, KeeperException {
    String znodePrefix = ELECTION_NAMESPACE + "/c_";
    String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

    System.out.println("znode name is: " + znodeFullPath);
    this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
  }
  public void electLeader() throws InterruptedException, KeeperException {
    Stat predecessorStat = null;
    String predecessorZnodeName = "";
    while (predecessorStat == null) {
      List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
      Collections.sort(children);
      String smallestChild = children.get(0);
      if (smallestChild.equals(this.currentZnodeName)) {
        System.out.println("I am the leader and my name is: " + smallestChild);
        return;
      } else {
        System.out.println("I am not the leader and the leader is znode: " + smallestChild);
        int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
        predecessorZnodeName = children.get(predecessorIndex);
        predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
      }
    }
    System.out.println("Watching znode " + predecessorZnodeName);
    System.out.println();
  }
  public void connectToZookeeper() throws IOException {
    this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
  }
  @Override
  public void process(WatchedEvent watchedEvent) {
    switch (watchedEvent.getType()) {
      case None:
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
          System.out.println("Successfully connected to Zookeeper");
        } else {
          synchronized (zooKeeper) {
            System.out.println("Disconnecting from Zookeeper event");
            zooKeeper.notifyAll();
          }
        }
        break;
      case NodeDeleted:
        try{
          electLeader();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (KeeperException e) {
          e.printStackTrace();
        }
        break;
    }
  }
  public void run() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.wait();
    }
  }
  public void close() throws InterruptedException {
    zooKeeper.close();
  }
}
