package org.kairosdb.datastore.remote;

import org.kairosdb.core.exception.DatastoreException;

import java.io.File;
import java.io.IOException;

/**
 * Remote Kairos node.
 */
public interface RemoteHost
{
	/**
	 * Sends the specified zip file to a remote Kairos node.
	 *
	 * @param zipFile file to send
	 * @throws IOException if file could not be sent
	 */
	void sendZipFile(File zipFile) throws IOException;

	/**
	 * Returns the Kairos version of the remote host.
	 *
	 * @throws DatastoreException if the remote host could not be contacted
	 */
	void getKairosVersion() throws DatastoreException;
}
