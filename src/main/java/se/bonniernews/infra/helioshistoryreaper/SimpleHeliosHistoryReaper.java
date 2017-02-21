package se.bonniernews.infra.helioshistoryreaper;

import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

// This class is adapted from com.spotify.helios.master.reaper.JobHistoryReaper
class SimpleHeliosHistoryReaper {
    private ZooKeeperClient client;

    SimpleHeliosHistoryReaper(final ZooKeeperClient client) {
        this.client = client;
    }

    private Job getJob(final JobId id) {
        final String path = Paths.configJob(id);
        try {
            final byte[] data = client.getData(path);
            return Json.read(data, Job.class);
        } catch (KeeperException.NoNodeException e) {
            // Return null to indicate that the job does not exist
            return null;
        } catch (KeeperException | IOException e) {
            throw new HeliosRuntimeException("getting job " + id + " failed", e);
        }
    }

    List<String> getJobs() {
        final String path = Paths.historyJobs();
        List<String> jobIds = Collections.emptyList();

        try {
            jobIds = client.getChildren(path);
        } catch (KeeperException e) {
            System.err.printf("Failed to get children of znode %1$s\t\n%2$s\n", path, e);
        }

        return jobIds;
    }

    boolean wouldRemoveItem(final String jobId) {
        System.out.printf("Deciding whether to reap job history for job %1$s\n", jobId);
        final JobId id = JobId.fromString(jobId);
        final Job job = getJob(id);
        return job == null;
    }

    void removeJob(final String jobId) {
        System.out.printf("Deciding whether to reap job history for job %1$s\n", jobId);
        final JobId id = JobId.fromString(jobId);
        final Job job = getJob(id);
        if (job == null) {
            try {
                client.deleteRecursive(Paths.historyJob(id));
                System.out.printf("Reaped job history for job %1$s\n", jobId);
            } catch (KeeperException.NoNodeException ignored) {
                // Something deleted the history right before we got to it. Ignore and keep going.
            } catch (KeeperException e) {
                System.err.printf("error reaping job history for job %1$s\n\t%2$s\n", jobId, e);
            }
        }
    }
}
