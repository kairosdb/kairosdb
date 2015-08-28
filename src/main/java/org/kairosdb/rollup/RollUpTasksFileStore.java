package org.kairosdb.rollup;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.io.FileUtils;
import org.kairosdb.core.http.rest.json.QueryParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

/**
 * Manages access to the roll up task store
 */
public class RollUpTasksFileStore implements RollUpTasksStore
{
	private static final String FILE_NAME = "rollup.config";

	private final ReentrantLock lock = new ReentrantLock();
	private final QueryParser parser;
	private final File configFile;

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Inject
	public RollUpTasksFileStore(@Named("STORE_DIRECTORY") String storeDirectory,
			QueryParser parser) throws IOException
	{
		checkNotNullOrEmpty(storeDirectory);
		checkNotNull(parser);

		configFile = new File(storeDirectory, FILE_NAME); // todo need to create the dir if it doesn't exist?
		configFile.createNewFile();
		this.parser = parser;
	}

	@Override
	public void write(List<RollUpTask> tasks) throws RollUpException
	{
		List<RollUpTask> existingTasks = read();
		existingTasks.removeAll(tasks);
		existingTasks.addAll(tasks);

		String json = parser.getGson().toJson(existingTasks);

		lock.lock();
		try
		{
			FileUtils.writeStringToFile(configFile, json, Charset.forName("UTF-8"));
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

	@Override
	public List<RollUpTask> read() throws RollUpException
	{
		lock.lock();
		try
		{
			String json = FileUtils.readFileToString(configFile, Charset.forName("UTF-8"));

			List<RollUpTask> rollUpTasks = parser.parseRollUpTask(json);
			if (rollUpTasks == null)
				rollUpTasks = new ArrayList<RollUpTask>();
			return rollUpTasks;
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
