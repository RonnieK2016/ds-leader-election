package com.udemy.distrsystems;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

@Slf4j
public class LeaderElection implements Watcher {

    public static final String ZOOKEEPER_ADDRESS = "127.0.0.1:2181";
    public static final int ZOOKEPER_SESSION_TIMEOUT = 3000;

    private ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectZookeeper();
        leaderElection.run();
        leaderElection.close();
        log.info("Exiting LeaderElection");
    }

    public void connectZookeeper() throws IOException {
        this.zk = new ZooKeeper(ZOOKEEPER_ADDRESS, ZOOKEPER_SESSION_TIMEOUT, this);
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
                }
                else {
                    synchronized (zk) {
                        log.info("Zookeeper Disconnected");
                        zk.notifyAll();
                    }
                }
        }
    }
}
