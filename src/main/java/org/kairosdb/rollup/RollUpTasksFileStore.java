package org.kairosdb.rollup;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.io.FileUtils;
import org.kairosdb.core.http.rest.QueryException;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

/**
 Manages access to the roll up task store
 */
public class RollUpTasksFileStore implements RollUpTasksStore
{
	private static final Logger logger = LoggerFactory.getLogger(RollUpTasksFileStore.class);
	private static final String FILE_NAME = "rollup.config";

	private final ReentrantLock lock = new ReentrantLock();
	private final QueryParser parser;
	private final File configFile;
	private final Map<String, RollupTask> rollups = new HashMap<String, RollupTask>();

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Inject
	public RollUpTasksFileStore(@Named("STORE_DIRECTORY") String storeDirectory,
			QueryParser parser) throws IOException, RollUpException
	{
		checkNotNullOrEmpty(storeDirectory);
		checkNotNull(parser);

		configFile = new File(storeDirectory, FILE_NAME); // todo need to create the dir if it doesn't exist?
		configFile.createNewFile();
		this.parser = parser;

		readFromFile();
	}

	@Override
	public void write(List<RollupTask> tasks) throws RollUpException
	{
		lock.lock();
		try
		{
			for (RollupTask task : tasks)
			{
				rollups.put(task.getId(), task);
			}

			writeTasks();
		}
		catch (IOException e)
		{
			throw new RollUpException("Failed to write roll up tasks to " + configFile.getAbsolutePath(), e);
		}
		finally
		{
			lock.unlock();
		}
	}

	private void writeTasks() throws IOException
	{
		FileUtils.deleteQuietly(configFile);
		for (RollupTask task : rollups.values())
		{
			FileUtils.writeLines(configFile, ImmutableList.of(task.getJson()), true);
		}
	}

	@Override
	public List<RollupTask> read() throws RollUpException
	{
		lock.lock();
		try
		{
			return new ArrayList<RollupTask>(rollups.values());
		}
		finally
		{
			lock.unlock();
		}
	}

	private void readFromFile() throws RollUpException
	{
		lock.lock();
		try
		{
			List<String> taskJson = FileUtils.readLines(configFile, Charset.forName("UTF-8"));
			for (String json : taskJson)
			{
				try
				{
					RollupTask task = parser.parseRollupTask(json);
					if (task != null)
						rollups.put(task.getId(), task);
				}
				catch (QueryException e)
				{
					logger.error("Could no parse rollup task from json: " + json);
				}
			}
		}
		catch (IOException e)
		{
			throw new RollUpException("Failed to read roll up tasks from " + configFile.getAbsolutePath(), e);
		}
		finally
		{
			lock.unlock();
		}
	}

	@Override
	public void remove(String id) throws IOException
	{
		lock.lock();
		try
		{
			rollups.remove(id);
			writeTasks();
		}
		finally
		{
			lock.unlock();
		}
	}

	@Override
	public long lastModifiedTime() throws RollUpException
	{
		lock.lock();
		try
		{
			if (!configFile.exists())
				return 0;
			else
				return configFile.lastModified();
		}
		finally
		{
			lock.unlock();
		}
	}
}
