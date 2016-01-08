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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.rollup.RollupTaskChangeListener.Action;
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
	private final CopyOnWriteArrayList<RollupTaskChangeListener> listenerList = new CopyOnWriteArrayList<RollupTaskChangeListener>();

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Inject
	public RollUpTasksFileStore(@Named("STORE_DIRECTORY") String storeDirectory,
			QueryParser parser) throws IOException, RollUpException
	{
		checkNotNullOrEmpty(storeDirectory);
		checkNotNull(parser);

		createStoreDirectory(storeDirectory);
		configFile = new File(storeDirectory, FILE_NAME);
		configFile.createNewFile();
		this.parser = parser;

		readFromFile();
	}

	@Override
	public void write(List<RollupTask> tasks) throws RollUpException
	{
		checkNotNull(tasks);
		List<RollupTask> added = new ArrayList<RollupTask>();
		List<RollupTask> changed = new ArrayList<RollupTask>();

		lock.lock();
		try
		{
			for (RollupTask task : tasks)
			{
				if (rollups.containsKey(task.getId()))
				{
					changed.add(task);
				}
				else
				{
					added.add(task);
				}
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

		notifyListeners(added, Action.ADDED);
		notifyListeners(changed, Action.CHANGED);
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
		checkNotNullOrEmpty(id);
		RollupTask removed = null;
		lock.lock();
		try
		{
			removed = rollups.get(id);
			if (removed != null)
			{
				rollups.remove(id);
				writeTasks();
			}
		}
		finally
		{
			lock.unlock();
		}

		if (removed != null)
		{
			notifyListeners(Collections.singletonList(removed), Action.REMOVED);
		}
	}

	public void addListener(RollupTaskChangeListener listener)
	{
		listenerList.add(listener);
	}

	public void notifyListeners(List<RollupTask> tasks, Action action)
	{
		for (RollupTask task : tasks)
		{
			for (RollupTaskChangeListener listener : listenerList)
			{
				listener.change(task, action);
			}
		}
	}

	private void createStoreDirectory(String storeDirectory) throws IOException
	{
		try
		{
			Files.createDirectory(Paths.get(storeDirectory));
		}
		catch (FileAlreadyExistsException ignore)
		{
		}
	}
}
