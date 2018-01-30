package org.kairosdb.rollup;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.HostManager;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.util.SummingMap;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

public class AssignmentManagerTest extends RollupTestBase
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private HostManager mockHostManager;

    private RollupTaskStatusStore statusStore = new RollupTaskStatusStoreImpl(fakeServiceKeyStore);
    private BalancingAlgorithm balancingAlgorithm = new ScoreBalancingAlgorithm();
    private AssignmentManager manager;

    @Before
    public void setup() throws RollUpException
    {
        manager = new AssignmentManager(LOCAL_HOST, taskStore, assignmentStore, statusStore, mockExecutionService, mockHostManager, balancingAlgorithm, 10);
    }

    @Test
    public void testConstructor_guid_null_invalid() throws RollUpException
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("guid cannot be null or empty");

        new AssignmentManager(null, taskStore, assignmentStore, statusStore, mockExecutionService, mockHostManager, balancingAlgorithm, 10);
    }

    @Test
    public void testConstructor_hostname_empty_invalid() throws RollUpException
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("guid cannot be null or empty");

        new AssignmentManager("", taskStore, assignmentStore, statusStore, mockExecutionService, mockHostManager, balancingAlgorithm, 10);
    }

    @Test
    public void testConstructor_taskStore_null_invalid() throws RollUpException
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("taskStore cannot be null");

        new AssignmentManager("guid", null, assignmentStore, statusStore, mockExecutionService, mockHostManager, balancingAlgorithm, 10);
    }

    @Test
    public void testConstructor_assignmentStore_null_invalid() throws RollUpException
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("assignmentStore cannot be null");

        new AssignmentManager("guid", taskStore, null, statusStore, mockExecutionService, mockHostManager, balancingAlgorithm, 10);
    }

    @Test
    public void testConstructor_statusStore_null_invalid() throws RollUpException
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("statusStore cannot be null");

        new AssignmentManager("guid", taskStore, assignmentStore, null, mockExecutionService, mockHostManager, balancingAlgorithm, 10);
    }

    @Test
    public void testConstructor_executor_null_invalid() throws RollUpException
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("executorService cannot be null");

        new AssignmentManager("guid", taskStore, assignmentStore, statusStore,null, mockHostManager, balancingAlgorithm, 10);
    }

    @Test
    public void testConstructor_balancing_null_invalid() throws RollUpException
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("balancing cannot be null");

        new AssignmentManager("guid", taskStore, assignmentStore, statusStore, mockExecutionService, mockHostManager,null, 10);
    }

    @Test
    public void test_removeAssignmentForDeletedTasks()
            throws RollUpException, DatastoreException
    {
        setupActiveHosts(LOCAL_HOST);
        addTasks(TASK1, TASK2, TASK3);
        addStatuses(TASK1, TASK2, TASK3);

        assertNotNull(statusStore.read(TASK1.getId()));
        assertNotNull(statusStore.read(TASK2.getId()));
        assertNotNull(statusStore.read(TASK3.getId()));

        manager.checkAssignmentChanges();

        // Remove task
        removeTasks(TASK2);

        manager.checkAssignmentChanges();

        assertThat(getAssignedHost(TASK1.getId()), equalTo(LOCAL_HOST));
        assertNull(getAssignedHost(TASK2.getId()));
        assertThat(getAssignedHost(TASK3.getId()), equalTo(LOCAL_HOST));
        assertNull(statusStore.read(TASK2.getId()));
    }

    @Test
    public void test_reassignTasksForInactiveHosts()
            throws RollUpException, DatastoreException
    {
        setupActiveHosts(LOCAL_HOST, "hostname1", "hostname2");
        addTasks(TASK1, TASK2, TASK3);

        manager.checkAssignmentChanges();

        // All servers are assigned 1 task
        List<Long> serverAssignmentCounts = getServerAssignmentCounts(assignmentStore.getAssignments());
        assertThat(serverAssignmentCounts.size(), equalTo(3));
        assertThat(serverAssignmentCounts.get(0), equalTo(1L));
        assertThat(serverAssignmentCounts.get(1), equalTo(1L));
        assertThat(serverAssignmentCounts.get(2), equalTo(1L));

        setupActiveHosts(LOCAL_HOST, "hostname1");

        manager.checkAssignmentChanges();

        // One server is assigned 1 task and the other 2 tasks
        serverAssignmentCounts = getServerAssignmentCounts(assignmentStore.getAssignments());
        assertThat(serverAssignmentCounts.size(), equalTo(2));
        assertTrue(serverAssignmentCounts.contains(1L));
        assertTrue(serverAssignmentCounts.contains(2L));
    }

    @Test
    public void test_addUnassignedTasks()
            throws RollUpException, DatastoreException
    {
        setupActiveHosts(LOCAL_HOST);
        addTasks(TASK1, TASK2, TASK3);

        manager.checkAssignmentChanges();

        // Add Task
        addTasks(TASK4);

        manager.checkAssignmentChanges();

        assertThat(assignmentStore.getAssignments().get(TASK4.getId()), equalTo(LOCAL_HOST));
    }

    @Test
    public void test_addUnassignedAndRemoveReasignedAndUnassignRemovedTasks()
            throws RollUpException, DatastoreException
    {
        setupActiveHosts(LOCAL_HOST, "hostname1", "hostname2");
        addTasks(TASK1, TASK2, TASK3);
        assignmentStore.setAssignment(TASK1.getId(), LOCAL_HOST);
        assignmentStore.setAssignment(TASK2.getId(), "hostname1");
        assignmentStore.setAssignment(TASK3.getId(), "hostname2");

        manager.checkAssignmentChanges();

        // Remove and Add task
        removeTasks(TASK2);
        setupActiveHosts(LOCAL_HOST, "hostname1");
        addTasks(TASK4);

        manager.checkAssignmentChanges();

        // One server is assigned 1 task and the other 2 tasks
        List<Long> serverAssignmentCounts = getServerAssignmentCounts(assignmentStore.getAssignments());
        assertThat(serverAssignmentCounts.size(), equalTo(2));
        assertTrue(serverAssignmentCounts.contains(1L));
        assertTrue(serverAssignmentCounts.contains(2L));

        assertNotNull(assignmentStore.getAssignments().get(TASK1.getId()));
        assertNull(assignmentStore.getAssignments().get(TASK2.getId()));
        assertNotNull(assignmentStore.getAssignments().get(TASK3.getId()));
        assertNotNull(assignmentStore.getAssignments().get(TASK4.getId()));
    }

    @Test
    public void test_rebalance()
            throws RollUpException, DatastoreException
    {
        setupActiveHosts(LOCAL_HOST, "hostname1", "hostname2");
        addTasks(TASK1, TASK2, TASK3, TASK4, TASK5);
        assignmentStore.setAssignment(TASK1.getId(), "hostname1");
        assignmentStore.setAssignment(TASK2.getId(), "hostname2");
        assignmentStore.setAssignment(TASK3.getId(), "hostname1");
        assignmentStore.setAssignment(TASK4.getId(), "hostname2");
        assignmentStore.setAssignment(TASK5.getId(), "hostname1");

        manager.checkAssignmentChanges();

        // One server is assigned 1 task and the other 2 have 2 tasks
        List<Long> serverAssignmentCounts = getServerAssignmentCounts(assignmentStore.getAssignments());
        assertThat(serverAssignmentCounts.size(), equalTo(3));
        assertTrue(serverAssignmentCounts.contains(1L));
        assertTrue(serverAssignmentCounts.contains(2L));
        assertTrue(serverAssignmentCounts.contains(2L));
    }

    @Test
    public void test_score()
    {
        RollupTask task1 = new RollupTask("1", new Duration(1, TimeUnit.SECONDS), ImmutableList.of(new Rollup()));
        RollupTask task2 = new RollupTask("2", new Duration(10, TimeUnit.SECONDS), ImmutableList.of(new Rollup()));
        RollupTask task3 = new RollupTask("3", new Duration(1, TimeUnit.MINUTES), ImmutableList.of(new Rollup()));
        RollupTask task4 = new RollupTask("4", new Duration(1, TimeUnit.HOURS), ImmutableList.of(new Rollup()));
        RollupTask task5 = new RollupTask("5", new Duration(60, TimeUnit.MINUTES), ImmutableList.of(new Rollup()));
        RollupTask task6 = new RollupTask("6", new Duration(60, TimeUnit.SECONDS), ImmutableList.of(new Rollup()));
        RollupTask task7 = new RollupTask("7", new Duration(60, TimeUnit.MONTHS), ImmutableList.of(new Rollup()));
        RollupTask task8 = new RollupTask("8", new Duration(60, TimeUnit.YEARS), ImmutableList.of(new Rollup()));

        assertThat(AssignmentManager.score(task1), equalTo(120L));
        assertThat(AssignmentManager.score(task2), equalTo(111L));
        assertThat(AssignmentManager.score(task3), equalTo(60L));
        assertThat(AssignmentManager.score(task4), equalTo(1L));
        assertThat(AssignmentManager.score(task5), equalTo(1L));
        assertThat(AssignmentManager.score(task6), equalTo(61L));
        assertThat(AssignmentManager.score(task7), equalTo(1L));
        assertThat(AssignmentManager.score(task8), equalTo(1L));
    }

    private void setupActiveHosts(String... hosts)
    {
        Date now = new Date();
        Map<String, ServiceKeyValue> hostMap = new HashMap<>();
        for (String host : hosts) {
            hostMap.put(host, new ServiceKeyValue(host, now));
        }

        when(mockHostManager.getActiveKairosHosts()).thenReturn(hostMap);
    }

    private String getAssignedHost(String taskId)
            throws DatastoreException
    {
        ServiceKeyValue value = fakeServiceKeyStore.getValue(RollUpAssignmentStoreImpl.SERVICE, RollUpAssignmentStoreImpl.SERVICE_KEY_ASSIGNMENTS, taskId);
        if (value != null)
        {
            return value.getValue();
        }
        return null;
    }

    private List<Long> getServerAssignmentCounts(Map<String, String> assignments)
    {
        SummingMap counts = new SummingMap();
        for (String server : assignments.values()) {
            counts.put(server, 1L);
        }
        ArrayList<Long> list = new ArrayList<>(counts.values());
        Collections.sort(list);
        return list;
    }

    private void addStatuses(RollupTask... tasks)
            throws RollUpException
    {
        for (RollupTask task : tasks) {
            statusStore.write(task.getId(), new RollupTaskStatus(new Date(), LOCAL_HOST));
        }
    }
}