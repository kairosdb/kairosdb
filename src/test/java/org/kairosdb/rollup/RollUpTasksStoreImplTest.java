package org.kairosdb.rollup;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.QueryException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RollUpTasksStoreImplTest extends RollupTestBase
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private RollUpTasksStore store;

    @Before
    public void setup()
            throws RollUpException, DatastoreException
    {
        store = new RollUpTasksStoreImpl(fakeServiceKeyStore, queryParser);
    }

    @Test
    public void test_writeRead()
            throws RollUpException, DatastoreException
    {
        addTasks(TASK1, TASK2, TASK3);

        store.write(ImmutableList.of(TASK1, TASK2, TASK3, TASK4));

        Map<String, RollupTask> tasks = store.read();
        assertThat(tasks.size(), equalTo(4));
        assertThat(tasks, hasEntry(TASK1.getId(), TASK1));
        assertThat(tasks, hasEntry(TASK2.getId(), TASK2));
        assertThat(tasks, hasEntry(TASK3.getId(), TASK3));
        assertThat(tasks, hasEntry(TASK4.getId(), TASK4));
    }

    @Test
    public void test_listIds()
            throws RollUpException, DatastoreException
    {
        addTasks(TASK1, TASK2, TASK3);

        store.write(ImmutableList.of(TASK1, TASK2, TASK3, TASK4));

        Set<String> ids = store.listIds();
        assertThat(ids.size(), equalTo(4));
        assertThat(ids, hasItem(TASK1.getId()));
        assertThat(ids, hasItem(TASK2.getId()));
        assertThat(ids, hasItem(TASK3.getId()));
        assertThat(ids, hasItem(TASK4.getId()));
    }

    @Test
    public void test_remove()
            throws RollUpException, DatastoreException
    {
        addTasks(TASK1, TASK2, TASK3, TASK4);

        store.write(ImmutableList.of(TASK1, TASK2, TASK3, TASK4));

        Map<String, RollupTask> tasks = store.read();
        assertThat(tasks.size(), equalTo(4));
        assertThat(tasks, hasEntry(TASK1.getId(), TASK1));
        assertThat(tasks, hasEntry(TASK2.getId(), TASK2));
        assertThat(tasks, hasEntry(TASK3.getId(), TASK3));
        assertThat(tasks, hasEntry(TASK4.getId(), TASK4));

        store.remove(TASK2.getId());
        store.remove(TASK3.getId());

        tasks = store.read();
        assertThat(tasks.size(), equalTo(2));
        assertThat(tasks, hasEntry(TASK1.getId(), TASK1));
        assertThat(tasks, hasEntry(TASK4.getId(), TASK4));
    }

    @Test
    public void test_import()
            throws IOException, RollUpException, QueryException
    {
        String oldFormat = Resources.toString(Resources.getResource("rollup_old_format.config"), Charsets.UTF_8);
        String[] lines = oldFormat.split("\n");
        List<RollupTask> oldTasks = new ArrayList<>();
        for (String line : lines) {
            oldTasks.add(queryParser.parseRollupTask(line));
        }

        Path path = Paths.get(RollUpTasksStoreImpl.OLD_FILENAME);
        try {
            Files.write(path, oldFormat.getBytes());
            assertTrue(Files.exists(path));

            RollUpTasksStoreImpl store = new RollUpTasksStoreImpl(fakeServiceKeyStore, queryParser);

            Map<String, RollupTask> tasks = store.read();
            assertThat(tasks.size(), equalTo(2));
            for (RollupTask oldTask : oldTasks) {
                assertThat(tasks.values(), hasItem(oldTask));
            }
        }
        finally {
            if (Files.exists(path)) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void test_import_oldFileNotExists()
            throws IOException, RollUpException, QueryException
    {
        Path path = Paths.get(RollUpTasksStoreImpl.OLD_FILENAME);
        assertFalse(Files.exists(path));

        RollUpTasksStoreImpl store = new RollUpTasksStoreImpl(fakeServiceKeyStore, queryParser);

        Map<String, RollupTask> tasks = store.read();
        assertThat(tasks.size(), equalTo(0));
    }
}