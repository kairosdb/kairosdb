package org.kairosdb.core.datastore;

import org.kairosdb.core.exception.DatastoreException;

import java.util.Date;

/**
 Created by bhawkins on 4/29/17.
 */
public interface ServiceKeyStore
{
	void setValue(String service, String serviceKey, String key, String value) throws DatastoreException;

	ServiceKeyValue getValue(String service, String serviceKey, String key) throws DatastoreException;

	Iterable<String> listServiceKeys(String service) throws DatastoreException;

	Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException;

	Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException;

	void deleteKey(String service, String serviceKey, String key) throws DatastoreException;

	/**
	 * Returns the time the service key was modified.
	 */
	Date getServiceKeyLastModifiedTime(String service, String serviceKey) throws DatastoreException;
}
