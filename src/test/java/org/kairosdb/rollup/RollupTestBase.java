package org.kairosdb.rollup;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;
import org.kairosdb.core.processingstage.TestKairosDBProcessor;
import org.kairosdb.testing.FakeServiceKeyStore;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.MockitoAnnotations.initMocks;

public abstract class RollupTestBase
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static final String LOCAL_HOST = "hostname0";
    static final RollupTask TASK1 = createTask("1", "task1");
    static final RollupTask TASK2 = createTask("2", "task2");
    static final RollupTask TASK3 = createTask("3", "task3");
    static final RollupTask TASK4 = createTask("4", "task4");
    static final RollupTask TASK5 = createTask("5", "task5");

    @Mock
    ScheduledExecutorService mockExecutionService;

    FakeServiceKeyStore fakeServiceKeyStore = new FakeServiceKeyStore();
    RollUpAssignmentStore assignmentStore = new RollUpAssignmentStoreImpl(fakeServiceKeyStore);
    RollUpTasksStore taskStore;
    QueryParser queryParser;

    @Before
    public void setupBase()
            throws KairosDBException
    {
        initMocks(this);
        queryParser = new QueryParser(new TestKairosDBProcessor(ImmutableList.of(new TestAggregatorFactory())), new TestQueryPluginFactory());
        taskStore = new RollUpTasksStoreImpl(fakeServiceKeyStore,
                queryParser);
    }

    private static RollupTask createTask(String id, String name)
    {
        Duration duration = new Duration(1, TimeUnit.HOURS);
        List<Rollup> rollups = new ArrayList<>();
        rollups.add(new Rollup());
        return new RollupTask(id, name, duration, rollups, "{\"id\": " + id + ", \"name\": \"" + name + "\", \"execution_interval\": {\"value\": 1, \"unit\": \"hours\"}}");
    }

    void addTasks(RollupTask... tasks)
            throws DatastoreException
    {
        for (RollupTask task : tasks) {
            fakeServiceKeyStore.setValue(RollUpTasksStoreImpl.SERVICE, RollUpTasksStoreImpl.SERVICE_KEY_CONFIG, task.getId(), task.getJson());
        }
    }
    void removeTasks(RollupTask... tasks)
            throws DatastoreException
    {
        for (RollupTask task : tasks) {
            fakeServiceKeyStore.deleteKey(RollUpTasksStoreImpl.SERVICE, RollUpTasksStoreImpl.SERVICE_KEY_CONFIG, task.getId());
        }
    }
}
