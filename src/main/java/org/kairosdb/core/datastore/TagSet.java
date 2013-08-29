package org.kairosdb.core.datastore;

import java.util.Set;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 8/28/13
 Time: 12:35 PM
 To change this template use File | Settings | File Templates.
 */
public interface TagSet
{
	/**
	 Returns a set of tag names associated with this group of data points
	 @return Set of tag names
	 */
	Set<String> getTagNames();

	/**
	 Returns the tag values for the given tag name.  After a grouping or aggregation
	 occurs a tag could have multiple values.
	 @param tag Tag to get the value for
	 @return A set of tag values
	 */
	Set<String> getTagValues(String tag);
}
