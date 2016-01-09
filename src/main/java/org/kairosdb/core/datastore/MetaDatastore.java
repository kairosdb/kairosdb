package org.kairosdb.core.datastore;

import org.kairosdb.core.exception.DatastoreException;

/**
 Created by bhawkins on 12/19/15.
 */
public interface MetaDatastore
{
	public String getValue(String namespace, String key) throws DatastoreException;

	public void setValue(String namespace, String key, String value) throws DatastoreException;

	public Iterable<String> getKeys(String namespace) throws DatastoreException;

	public Iterable<String> getKeysWithPrefix(String namespace, String prefix) throws DatastoreException;

	public Iterable<String> getNamespaces() throws DatastoreException;

	public void deleteValue(String namespace, String metaKey);
}
