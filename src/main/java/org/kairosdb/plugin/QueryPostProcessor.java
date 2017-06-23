package org.kairosdb.plugin;

import java.io.File;

/**
 Created by bhawkins on 5/18/17.
 */
public interface QueryPostProcessor
{
	File postProcessResults(File queryResults);
}
