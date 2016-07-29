/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A EC2 aware Round-robin load balancing policy.
 * <p/>
 * This policy provides round-robin queries over the node of the local
 * region and availability zone. It also includes in the query plans returned a configurable
 * number of hosts in the remote regions, but those are always tried
 * after the local nodes. In other words, this policy guarantees that no
 * host in a remote region will be queried unless no host in the local
 * region can be reached.
 * <p/>
 * If used with a single region and availability zone, this policy is equivalent to the
 * {@code LoadBalancingPolicy.RoundRobin} policy, but its EC2 awareness
 * incurs a slight overhead so the {@code LoadBalancingPolicy.RoundRobin}
 * policy could be preferred to this policy in that case.
 */
public class EC2AwareRoundRobinPolicy implements LoadBalancingPolicy {

    private final ConcurrentMap<String, ConcurrentMap<String, CopyOnWriteArrayList<Host>>> perRegionLiveHosts =
            new ConcurrentHashMap<>();
    private final AtomicInteger index = new AtomicInteger();
    private final String localRegion;
    private final String localAvailabilityZone;
    private final int usedHostsPerRemoteRegion;

    private final static Logger LOG = LoggerFactory.getLogger(EC2AwareRoundRobinPolicy.class);

    public static String getHTML(String urlToRead) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlToRead);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        return result.toString();
    }

    public static EC2AwareRoundRobinPolicy CreateEC2AwareRoundRobinPolicy() {
        try {
            String az = getHTML("http://169.254.169.254/latest/meta-data/placement/availability-zone");
            String[] parts = az.split("-");
            LOG.info("Setting region={}-{} az={}", parts[0], parts[1], parts[2]);
            return new EC2AwareRoundRobinPolicy(parts[0]+"-"+parts[1], parts[2], 0);
        }
        catch(IOException ex) {
            throw new RuntimeException("Failed to get AZ from Meta Service", ex);
        }
    }

    public EC2AwareRoundRobinPolicy(String localRegion, String localAvailabilityZone) {
        this(localRegion, localAvailabilityZone, 0);
    }

    /**
     * Creates a new EC2AwareRoundRobin policy given the name of the local
     * region and that uses the provided number of host per remote
     * regions as failover for the local hosts.
     * <p/>
     The name of the local region provided must be the local
     * datacenter name as known by Cassandra and the availability
     * zone must be the name of the rack.
     *
     * @param localRegion the name of the local region (datacenter) (as known by
     *                    Cassandra).
     * @param localAvailabilityZone the name of the local availability zone (rack) (as known by
     *                    Cassandra).
     * @param usedHostsPerRemoteRegion the number of host per remote
     *                                 region that policies created by the returned factory should
     *                                 consider. Created policies {@code distance} method will return a
     *                                 {@code HostDistance.REMOTE} distance for only {@code
     *                                 usedHostsPerRemoteRegion} hosts per remote region. Other hosts
     *                                 of the remote regions will be ignored (and thus no
     *                                 connections to them will be maintained).
     */
    public EC2AwareRoundRobinPolicy(String localRegion, String localAvailabilityZone, int usedHostsPerRemoteRegion) {
        this.localRegion = localRegion;
        this.usedHostsPerRemoteRegion = usedHostsPerRemoteRegion;
        this.localAvailabilityZone = localAvailabilityZone;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts) {
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));

        for (Host host : hosts) {
            putHost(host);
        }
    }

    private String dc(Host host) {
        String dc = host.getDatacenter();
        return dc == null ? localRegion : dc;
    }

    private String az(Host host) {
        String az = host.getRack();
        return az == null ? localAvailabilityZone : az;
    }

    @SuppressWarnings("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list) {
        return (CopyOnWriteArrayList<Host>) list.clone();
    }

    /**
     * Return the HostDistance for the provided host.
     * <p/>
     * This policy consider nodes in the local availability zone as {@code LOCAL}.
     * Hosts within the same region but in another availability zone will be
     * considered {@code REMOTE}.
     * For each remote region, it considers a configurable number of
     * hosts as {@code REMOTE} and the rest is {@code IGNORED}.
     * <p/>
     * To configure how many host in each remote region is considered
     * {@code REMOTE}, see {@link #EC2AwareRoundRobinPolicy(String, String, int)}.
     *
     * @param host the host of which to return the distance of.
     *
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host) {
        String region = dc(host);
        String az = az(host);

        //Servers
        if (region.equals(localRegion) && az.equals(localAvailabilityZone))
            return HostDistance.LOCAL;
            //Servers in the same region but not in the same az should always be kept a connection too
        else if (region.equals(localRegion))
            return HostDistance.REMOTE;

        ConcurrentMap<String, CopyOnWriteArrayList<Host>> availabilityZones = perRegionLiveHosts.get(region);
        if (availabilityZones == null || usedHostsPerRemoteRegion == 0)
            return HostDistance.IGNORED;

        // Pull out all instances from all azs in the remote region
        CopyOnWriteArrayList<Host> regionHosts = new CopyOnWriteArrayList<Host>();
        for (CopyOnWriteArrayList<Host> hosts : availabilityZones.values()) {
            regionHosts.addAll(hosts);
        }

        return regionHosts.subList(0, Math.min(regionHosts.size(), usedHostsPerRemoteRegion)).contains(host)
                ? HostDistance.REMOTE
                : HostDistance.IGNORED;
    }



    /**
     * Returns the hosts to use for a new query.
     * <p/>
     * The returned plan will always try each known host in the local
     * region and local availability zone first, and then, if none of the local
     * host is reachable, will try the instances in the local region but remote
     * availability zone. If none of those are found, will try up to a configurable
     * number of other host per remote region.
     * The order of the local node in the returned query plan will follow a
     * Round-robin algorithm.
     *     *
     * @return a new query plan, i.e. an iterator indicating which host to
     *         try first for querying, which one to use as failover, etc...
     */
    @Override
    public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {

        ConcurrentMap<String, CopyOnWriteArrayList<Host>> localRegionLiveHosts = perRegionLiveHosts.get(localRegion);

        final List<Host> hosts = localRegionLiveHosts != null && localRegionLiveHosts.containsKey(localAvailabilityZone) ?
                cloneList(localRegionLiveHosts.get(localAvailabilityZone)) :
                Collections.<Host>emptyList();

        final int startIdx = index.getAndIncrement();

        return new AbstractIterator<Host>() {

            private int idx = startIdx;
            private int remainingLocal = hosts.size();

            // For remote Dcs
            private Iterator<String> remoteDcs;
            private List<Host> currentAzHosts;
            private int currentDcRemaining;

            @Override
            protected Host computeNext() {
                if (remainingLocal > 0) {
                    remainingLocal--;
                    int c = idx++ % hosts.size();
                    if (c < 0)
                        c += hosts.size();
                    return hosts.get(c);
                }
                if (remoteDcs == null) {
                    Set<String> copy = new HashSet<String>(perRegionLiveHosts.keySet());
                    copy.remove(localRegion);
                    remoteDcs = copy.iterator();
                    ConcurrentMap<String, CopyOnWriteArrayList<Host>> region = perRegionLiveHosts.get(localRegion);

                    if (region != null) {
                        // Add all the local region hosts that are not in the current AZ
                        currentAzHosts = new ArrayList<Host>();
                        for (Map.Entry<String, CopyOnWriteArrayList<Host>> az : region.entrySet()) {
                            if (!az.getKey().equals(localAvailabilityZone)) {
                                currentAzHosts.addAll(az.getValue());
                            }
                        }
                        currentDcRemaining = currentAzHosts.size();
                    }

                }

                if (currentAzHosts != null && currentDcRemaining > 0) {
                    currentDcRemaining--;
                    int c = idx++ % currentAzHosts.size();
                    if (c < 0)
                        c += currentAzHosts.size();
                    return currentAzHosts.get(c);
                }


                if (!remoteDcs.hasNext())
                    return endOfData();

                String nextRemoteRegion = remoteDcs.next();
                ConcurrentMap<String, CopyOnWriteArrayList<Host>> nextDcHosts = perRegionLiveHosts.get(nextRemoteRegion);
                if (nextDcHosts != null) {
                    // Clone for thread safety
                    List<Host> dcHosts = new ArrayList<Host>();
                    for (CopyOnWriteArrayList<Host> hosts : nextDcHosts.values()) {
                        dcHosts.addAll(hosts);
                    }
                    currentAzHosts = dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteRegion));
                    currentDcRemaining = currentAzHosts.size();
                }

                return computeNext();
            }
        };
    }

    @Override
    public void onUp(Host host) {
        putHost(host);
    }

    @Override
    public void onDown(Host host) {
        ConcurrentMap<String, CopyOnWriteArrayList<Host>> regionHosts = perRegionLiveHosts.get(dc(host));
        if (regionHosts != null) {
            CopyOnWriteArrayList<Host> azHosts = regionHosts.get(az(host));
            if (azHosts != null)
                azHosts.remove(host);
        }
    }

    @Override
    public void onAdd(Host host) {
        onUp(host);
    }

    @Override
    public void onRemove(Host host) {
        onDown(host);
    }

    private void putHost(Host host) {
        String dc = dc(host);
        String az = az(host);
        ConcurrentMap<String, CopyOnWriteArrayList<Host>> region = perRegionLiveHosts.get(dc);

        if (region == null) {
            region = new ConcurrentHashMap<>();
            ConcurrentMap<String, CopyOnWriteArrayList<Host>> oldRegion = perRegionLiveHosts.putIfAbsent(dc, region);
            if (oldRegion != null)
                region = oldRegion;
        }
        CopyOnWriteArrayList<Host> azList = region.get(az);
        if (azList == null) {
            azList = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<Host> oldAz = region.putIfAbsent(az, azList);
            if (oldAz != null)
                azList = oldAz;
        }
        azList.addIfAbsent(host);
    }

    @Override
    public void close() {

    }
}