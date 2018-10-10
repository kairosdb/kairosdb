package org.kairosdb.datastore.remote;

import com.google.common.collect.ImmutableSortedMap;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.DiskUtils;
import org.mockito.ArgumentMatcher;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

public class RemoteDatastoreTest
{
	private RemoteHost mockRemoteHost;
	private FilterEventBus mockEventBus;
	private DiskUtils mockDiskUtils;
	private File tempDir;
	private Publisher<DataPointEvent> mockPublisher;

	@SuppressWarnings("unchecked")
	@Before
	public void setup() throws IOException
	{
		tempDir = Files.createTempDirectory("RemoteDatastoreTestTempDir").toFile();
		mockRemoteHost = mock(RemoteHost.class);
		mockEventBus = mock(FilterEventBus.class);
		mockDiskUtils = mock(DiskUtils.class);
		mockPublisher = mock(Publisher.class);

		when(mockEventBus.createPublisher(DataPointEvent.class)).thenReturn(mockPublisher);
	}

	@After
	public void tearDown() throws IOException
	{
		FileUtils.deleteDirectory(tempDir);
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void test_cleanup() throws IOException, DatastoreException
	{
		when(mockDiskUtils.percentAvailable(any())).thenReturn(96L).thenReturn(96L).thenReturn(80L);
		RemoteDatastore remoteDatastore = new RemoteDatastore(tempDir.getAbsolutePath(), "95", mockRemoteHost, mockEventBus, mockDiskUtils);

		// Create zip files
		createZipFile("zipFile1.gz");
		createZipFile("zipFile2.gz");
		createZipFile("zipFile3.gz");
		createZipFile("zipFile4.gz");

		remoteDatastore.sendData(); // Note that sendData() will create an additional zip file (so 5 zips)

		// assert that temp dir only contains x number of zip files
		File[] files = tempDir.listFiles((dir, name) -> name.endsWith(".gz"));
		assertThat(files.length, equalTo(3));
		verify(mockPublisher, times(2)).post(argThat(
				new DataPointEventMatcher(createDataPoint("kairosdb.datastore.remote.deleted_zipFile_size", 14L))));
	}

	@Test
	public void test_sendData() throws IOException, DatastoreException
	{
		RemoteDatastore remoteDatastore = new RemoteDatastore(tempDir.getAbsolutePath(), "95", mockRemoteHost, mockEventBus, mockDiskUtils);

		remoteDatastore.sendData();

		verify(mockRemoteHost, times(1)).sendZipFile(any());
	}

	private DataPointEvent createDataPoint(String metricName, long value)
	{
		ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "localhost");
		return new DataPointEvent(metricName, tags, new LongDataPoint(System.currentTimeMillis(), value));
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private void createZipFile(String name) throws IOException
	{
		File file = new File(tempDir, name);
		file.createNewFile();
		Files.write(file.toPath(), "This is a test".getBytes());
	}

	private class DataPointEventMatcher implements ArgumentMatcher<DataPointEvent>
	{
		private DataPointEvent event;
		private String errorMessage;

		DataPointEventMatcher(DataPointEvent event)
		{
			this.event = event;
		}

		@Override
		public boolean matches(DataPointEvent dataPointEvent)
		{
			if (!event.getMetricName().equals(dataPointEvent.getMetricName()))
			{
				errorMessage = "Metric names don't match: " + event.getMetricName() + " != " + dataPointEvent.getMetricName();
				return false;
			}
			if (!event.getTags().equals(dataPointEvent.getTags()))
			{
				errorMessage = "Tags don't match: " + event.getTags() + " != " + dataPointEvent.getTags();
				return false;
			}
			if (event.getDataPoint().getDoubleValue() != dataPointEvent.getDataPoint().getDoubleValue())
			{
				errorMessage = "Data points don't match: " + event.getDataPoint().getDoubleValue() + " != " + dataPointEvent.getDataPoint().getDoubleValue();
				return false;
			}
			return true;
		}

		@Override
		public String toString()
		{
			if (errorMessage != null)
			{
				return errorMessage;
			}
			return "";
		}
	}
}