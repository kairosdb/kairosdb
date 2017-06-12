package org.kairosdb.core.datastore;

import org.kairosdb.core.PluginException;

import java.io.File;
import java.io.IOException;

/**
 Created by bhawkins on 5/18/17.

 */
public interface QueryPostProcessingPlugin extends QueryPlugin
{
	File processQueryResults(File queryResults) throws IOException, PluginException;
}
