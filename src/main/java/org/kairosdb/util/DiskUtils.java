package org.kairosdb.util;

import java.io.File;

public interface DiskUtils
{
	/**
	 * Returns the percent of disk space that is available for the drive the specified file is on.
	 *
	 * @param file file
	 * @return percent of disk space available
	 */
	long percentAvailable(File file);
}
