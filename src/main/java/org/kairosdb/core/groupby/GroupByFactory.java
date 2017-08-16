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

import com.google.inject.Inject;
import com.google.inject.Injector;
import org.kairosdb.core.annotation.Feature;
import org.kairosdb.core.processingstage.GenericFeatureProcessorFactory;
import org.kairosdb.plugin.GroupBy;

import java.lang.reflect.InvocationTargetException;

@Feature(
        name = "group_by",
        label = "Group By"
)
public class GroupByFactory extends GenericFeatureProcessorFactory<GroupBy>
{
    @Inject
    public GroupByFactory(Injector injector)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        super(injector, GroupBy.class);
    }
}