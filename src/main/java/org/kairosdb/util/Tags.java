package org.kairosdb.util;

import com.google.common.collect.ImmutableSortedMap;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 12/10/13
 Time: 10:54 AM
 To change this template use File | Settings | File Templates.
 */
public class Tags
{
	public static ImmutableSortedMap.Builder<String, String> create()
	{
		return ImmutableSortedMap.<String, String>naturalOrder();
	}
}
