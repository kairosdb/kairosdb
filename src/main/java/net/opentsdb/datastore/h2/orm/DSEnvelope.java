// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package net.opentsdb.datastore.h2.orm;

import java.util.*;
import javax.sql.*;

import genorm.runtime.*;

public class DSEnvelope implements GenOrmDSEnvelope
	{
	private DataSource m_dataSource;
	private Map<String, GenOrmKeyGenerator> m_keyGenMap;
	
	public DSEnvelope(DataSource ds)
		{
		m_dataSource = ds;
		m_keyGenMap = new HashMap<String, GenOrmKeyGenerator>();
		m_keyGenMap.put("data_point", new net.opentsdb.datastore.h2.orm.DataPoint_base.DataPointKeyGenerator(ds));

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
