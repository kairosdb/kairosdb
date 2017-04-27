/*
 * Copyright 2016 KairosDB Authors
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
package org.kairosdb.core.reporting;

import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.servlet.ServletModule;
import org.kairosdb.core.http.MonitorFilter;

import java.util.List;
import java.util.Set;

public class MetricReportingModule extends ServletModule
{
	@Override
	protected void configureServlets()
	{
		bind(MetricReporterService.class).in(Singleton.class);

		bind(MonitorFilter.class).in(Scopes.SINGLETON);
		filter("/*").through(MonitorFilter.class);

		bind(DataPointsMonitor.class).in(Scopes.SINGLETON);

		KairosMetricReporterListProvider reporterProvider = new KairosMetricReporterListProvider();
		bind(KairosMetricReporterListProvider.class).toInstance(reporterProvider);

		bindListener(Matchers.any(), reporterProvider);
	}
}