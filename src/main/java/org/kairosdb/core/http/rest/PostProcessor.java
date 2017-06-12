package org.kairosdb.core.http.rest;

import java.io.File;

/**
 Created by bhawkins on 5/18/17.
 */
public interface PostProcessor
{
	File postProcessResults(File queryResults);
}
