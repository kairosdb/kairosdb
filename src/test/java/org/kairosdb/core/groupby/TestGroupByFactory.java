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
import org.kairosdb.core.annotation.Feature;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;
import org.kairosdb.plugin.GroupBy;

import java.util.HashMap;
import java.util.Map;

@Feature(
        name = "group_by",
        label = "Test GroupBy"
)
public class TestGroupByFactory implements FeatureProcessingFactory<GroupBy>
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
        String name = (groupBy.getClass().getAnnotation(FeatureComponent.class)).name();
        groupBys.put(name, groupBy);
    }

    @Override
    public GroupBy createFeatureProcessor(String name) { return groupBys.get(name); }

    @Override
    public Class<GroupBy> getFeature() { return GroupBy.class; }

    @Override
    public ImmutableList<FeatureProcessorMetadata> getFeatureProcessorMetadata() { return ImmutableList.copyOf(new FeatureProcessorMetadata[]{});}
}