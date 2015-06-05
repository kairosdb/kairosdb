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
package org.kairosdb.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CharacterSet
{
	private static final Pattern regex = Pattern.compile(".*[:=].*");

	private CharacterSet()
	{
	}

	/**
	 * Returns true if the specified string contains a valid set of characters
	 * For a tag name or value, cannot contain ; or =
	 * @param s string to test
	 * @return true if all characters in the string are valid
	 */
	public static boolean isValidTagNameValue(String s)
	{
		Matcher matcher = regex.matcher(s);
		return !matcher.matches();
	}
}