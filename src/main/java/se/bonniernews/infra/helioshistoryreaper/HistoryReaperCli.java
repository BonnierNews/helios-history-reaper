package se.bonniernews.infra.helioshistoryreaper;

import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class HistoryReaperCli {

    public static void main(String[] args) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(args[0])
                .connectionTimeoutMs((int) TimeUnit.SECONDS.toMillis(1))
                .sessionTimeoutMs((int) TimeUnit.MINUTES.toMillis(5))
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        DefaultZooKeeperClient zkClient = new DefaultZooKeeperClient(curatorFramework);
        zkClient.start();

        SimpleHeliosHistoryReaper historyReaper = new SimpleHeliosHistoryReaper(zkClient);
        List<String> jobs = historyReaper.getJobs();
        int toRemove = jobs.stream()
                .map(jid -> historyReaper.wouldRemoveItem(jid) ? 1 : 0)
                .reduce(0, (a, b) -> a + b);
        int total = jobs.size();

        System.out.printf("Will remove %1$d jobs, out of %2$d. Ok? [y/N]", toRemove, total);

        Scanner scanner = new Scanner(System.in);
        if (scanner.nextLine().equals("y")) {
            jobs.forEach(historyReaper::removeJob);
        } else {
            System.out.println("Exiting");
        }
    }
}

