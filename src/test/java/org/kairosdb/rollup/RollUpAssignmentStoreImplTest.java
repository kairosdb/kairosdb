    package org.kairosdb.rollup;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.testing.FakeServiceKeyStore;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class RollUpAssignmentStoreImplTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private FakeServiceKeyStore fakeKeyStore = new FakeServiceKeyStore();
    private RollUpAssignmentStore store = new RollUpAssignmentStoreImpl(fakeKeyStore);

    @Before
    public void setup()
            throws RollUpException
    {
        store.setAssignment("id1", "host1");
        store.setAssignment("id2", "host2");
        store.setAssignment("id3", "host3");
        store.setAssignment("id4", "host3");
        store.setAssignment("id5", "host3");
    }

    @Test
    public void test_constructor_nullKeystore_invalid()
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("serviceKeyStore cannot be null");

        new RollUpAssignmentStoreImpl(null);
    }

    @Test
    public void test_assigedIds()
            throws RollUpException
    {
        Collection<String> ids = store.getAssignedIds("host1");

        assertThat(ids.size(), equalTo(1));
        assertThat(ids, hasItems("id1"));

        ids = store.getAssignedIds("host2");

        assertThat(ids.size(), equalTo(1));
        assertThat(ids, hasItems("id2"));

        ids = store.getAssignedIds("host3");

        assertThat(ids.size(), equalTo(3));
        assertThat(ids, hasItems("id3", "id4", "id5"));
    }

    @Test
    public void test_assignmentIds()
            throws RollUpException
    {
        Set<String> assignmentIds = store.getAssignmentIds();

        assertThat(assignmentIds.size(), equalTo(5));
        assertThat(assignmentIds, hasItems("id1", "id2", "id3", "id4", "id5"));
    }

    @Test
    public void test_assignments()
            throws RollUpException
    {
        Map<String, String> assignments = store.getAssignments();

        assertThat(assignments.size(), equalTo(5));
        assertThat(assignments.get("id1"), equalTo("host1"));
        assertThat(assignments.get("id2"), equalTo("host2"));
        assertThat(assignments.get("id3"), equalTo("host3"));
        assertThat(assignments.get("id4"), equalTo("host3"));
        assertThat(assignments.get("id5"), equalTo("host3"));
    }

    @Test
    public void test_removeAssignments()
            throws RollUpException
    {
        store.removeAssignments(ImmutableSet.of("id2", "id4"));

        Map<String, String> assignments = store.getAssignments();

        assertThat(assignments.size(), equalTo(3));
        assertThat(assignments.get("id1"), equalTo("host1"));
        assertThat(assignments.get("id3"), equalTo("host3"));
        assertThat(assignments.get("id5"), equalTo("host3"));
    }
}