/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.kairosdb.core.aggregator.*;
import org.kairosdb.core.groupby.GroupByFactory;
import org.kairosdb.core.groupby.GuiceGroupByFactory;
import org.kairosdb.core.groupby.TimeGroupBy;
import org.kairosdb.core.groupby.ValueGroupBy;
import org.kairosdb.core.http.rest.json.GsonParser;

import java.util.Properties;

public class CoreModule extends AbstractModule
{
	public static final String DATASTORE_CLASS_PROPERTY = "kairosdb.datastore.class";
	private Properties m_props;

	public CoreModule(Properties props)
	{
		m_props = props;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void configure()
	{
		/*String dsClassName = m_props.getProperty(DATASTORE_CLASS_PROPERTY);
		try
		{
			Class dsClass = Class.forName(dsClassName);
			bind(Datastore.class).to(dsClass).in(Scopes.SINGLETON);
		}
		catch (ClassNotFoundException e)
		{
			throw new MissingResourceException("Unable to load Datastore class",
					dsClassName, DATASTORE_CLASS_PROPERTY);
		}*/
		bind(AggregatorFactory.class).to(GuiceAggregatorFactory.class).in(Singleton.class);
		bind(GroupByFactory.class).to(GuiceGroupByFactory.class).in(Singleton.class);
		bind(GsonParser.class).in(Singleton.class);

		bind(SumAggregator.class);
		bind(MinAggregator.class);
		bind(MaxAggregator.class);
		bind(AvgAggregator.class);
		bind(StdAggregator.class);
		bind(RateAggregator.class);

		bind(ValueGroupBy.class);
		bind(TimeGroupBy.class);

		Names.bindProperties(binder(), m_props);
	}
}
