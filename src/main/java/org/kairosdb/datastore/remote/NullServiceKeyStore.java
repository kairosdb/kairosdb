package org.kairosdb.datastore.remote;

import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.datastore.ServiceKeyValue;

import java.util.Date;

/**
 Created by bhawkins on 4/29/17.
 */
public class NullServiceKeyStore implements ServiceKeyStore
{
	@Override
	public void setValue(String service, String serviceKey, String key, String value) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public ServiceKeyValue getValue(String service, String serviceKey, String key) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public Iterable<String> listServiceKeys(String service) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public void deleteKey(String service, String serviceKey, String key) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}

	@Override
	public Date getServiceKeyLastModifiedTime(String service, String serviceKey) throws DatastoreException
	{
		throw new DatastoreException("Method not implemented.");
	}
}
