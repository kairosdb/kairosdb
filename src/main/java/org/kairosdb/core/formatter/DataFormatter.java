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
package org.kairosdb.core.formatter;

import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.metrics4j.collectors.LongCollector;

import java.io.Writer;
import java.util.List;

public interface DataFormatter
{
	void format(Writer writer, List<List<DataPointGroup>> data) throws FormatterException;

	void format(Writer writer, Iterable<String> iterable, LongCollector collector) throws FormatterException;
}