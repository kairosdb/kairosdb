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
package org.kairosdb.core.groupby;

import com.google.common.collect.ImmutableList;
import org.kairosdb.core.annotation.QueryProcessingStage;
import org.kairosdb.core.annotation.QueryProcessor;
import org.kairosdb.core.processingstage.QueryProcessingStageFactory;
import org.kairosdb.core.processingstage.metadata.QueryProcessorMetadata;

import java.util.HashMap;
import java.util.Map;

@QueryProcessingStage(
        name = "group_by",
        label = "Test GroupBy"
)
public class TestGroupByFactory implements QueryProcessingStageFactory<GroupBy>
{
    private Map<String, GroupBy> groupBys = new HashMap<String, GroupBy>();

    public TestGroupByFactory()
    {
        addGroupBy(new SimpleTimeGroupBy());
        addGroupBy(new ValueGroupBy());
        addGroupBy(new TagGroupBy());
        addGroupBy(new TimeGroupBy());
    }

    private void addGroupBy(GroupBy groupBy)
    {
        String name = (groupBy.getClass().getAnnotation(QueryProcessor.class)).name();
        groupBys.put(name, groupBy);
    }

    @Override
    public GroupBy createQueryProcessor(String name) { return groupBys.get(name); }

    @Override
    public Class<GroupBy> getQueryProcessorFamily() { return GroupBy.class; }

    @Override
    public ImmutableList<QueryProcessorMetadata> getQueryProcessorMetadata() { return ImmutableList.copyOf(new QueryProcessorMetadata[]{});}
}