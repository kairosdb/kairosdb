package org.kairosdb.util;

import com.google.inject.Inject;

import java.io.File;

public class DiskUtilsImpl implements DiskUtils
{
	@Inject
	public DiskUtilsImpl()
	{
	}

	@Override
	public long percentAvailable(File file)
	{
		return Math.round(((double) file.getFreeSpace() / file.getTotalSpace()) * 100);
	}
}
