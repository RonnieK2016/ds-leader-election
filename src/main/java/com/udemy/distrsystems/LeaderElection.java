package com.udemy.distrsystems;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Slf4j
public class LeaderElection implements Watcher {

    public static final String ZOOKEEPER_ADDRESS = "127.0.0.1:2181";
    public static final int ZOOKEEPER_SESSION_TIMEOUT = 3000;
    public static final String ELECTION_NAMESPACE = "/election";

    private ZooKeeper zk;
    private String zNodeIdx;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        leaderElection.run();
        leaderElection.close();
        log.info("Exiting LeaderElection");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String zNodePrefix = ELECTION_NAMESPACE + "/c_";
        String zNodeFullPath = zk.create(zNodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        log.info("Created znode {}.", zNodeFullPath);
        this.zNodeIdx = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reElectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        while (predecessorStat == null) {
            List<String> children = zk.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);
            if (smallestChild.equals(zNodeIdx)) {
                log.info("Current node is the leader");
                return;
            } else {
                int predecessorIdx = Collections.binarySearch(children, zNodeIdx) - 1;
                String predecessorName = children.get(predecessorIdx);
                predecessorStat = zk.exists(ELECTION_NAMESPACE + "/" + predecessorName, this);
                log.info("Current node is not the leader, watching - {}", predecessorName);
            }
        }
    }
    public void connectZookeeper() throws IOException {
        this.zk = new ZooKeeper(ZOOKEEPER_ADDRESS, ZOOKEEPER_SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zk) {
            zk.wait();
        }
    }

    public void close() throws InterruptedException {
        zk.close();
    }


    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    log.info("Zookeeper Connected");
                } else {
                    synchronized (zk) {
                        log.info("Zookeeper Disconnected");
                        zk.notifyAll();
                    }
                }
                break;
            case NodeDataChanged:
                log.info("Zookeeper node data changed event - {}", event);
                break;
            case NodeChildrenChanged:
                log.info("Zookeeper node children changed - {}", event);
                break;
            case NodeCreated:
                log.info("Zookeeper node created - {}", event);
                break;
            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
        }
    }
}
