package org.kairosdb.datastore.h2.orm;

import java.util.*;
import javax.sql.*;

import org.agileclick.genorm.runtime.*;

public class DSEnvelope implements GenOrmDSEnvelope
	{
	private DataSource m_dataSource;
	private Map<String, GenOrmKeyGenerator> m_keyGenMap;
	
	public DSEnvelope(DataSource ds)
		{
		m_dataSource = ds;
		m_keyGenMap = new HashMap<String, GenOrmKeyGenerator>();
		}
		
	public DataSource getDataSource()
		{
		return (m_dataSource);
		}
		
	public GenOrmKeyGenerator getKeyGenerator(String table)
		{
		return (m_keyGenMap.get(table));
		}
	
	public void initialize()
		{
		GenOrmDataSource.setDataSource(this);
		}
		
	/**
		Method for overriding the standard key generator
	*/
	public void setKeyGenerator(String table, GenOrmKeyGenerator generator)
		{
		m_keyGenMap.put(table, generator);
		}
	}
