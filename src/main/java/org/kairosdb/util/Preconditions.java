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

package org.kairosdb.util;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

public class Preconditions
{
	public static String checkNotNullOrEmpty(String reference)
	{
		com.google.common.base.Preconditions.checkNotNull(reference);
		if (reference.isEmpty())
		{
			throw new IllegalArgumentException();
		}
		return reference;
	}

	public static String checkNotNullOrEmpty(String reference,
	                                         @Nullable String errorMessageTemplate,
	                                         @Nullable Object... errorMessageArgs)
	{
		com.google.common.base.Preconditions.checkNotNull(reference, errorMessageTemplate, errorMessageArgs);
		if (reference.isEmpty())
		{
			throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));

		}
		return reference;
	}

	/**
	 * Copied from Google's Precondition class because it is package protected.
	 * <p/>
	 * Substitutes each {@code %s} in {@code template} with an argument. These
	 * are matched by position - the first {@code %s} gets {@code args[0]}, etc.
	 * If there are more arguments than placeholders, the unmatched arguments will
	 * be appended to the end of the formatted message in square braces.
	 *
	 * @param template a non-null string containing 0 or more {@code %s}
	 *                 placeholders.
	 * @param args     the arguments to be substituted into the message
	 *                 template. Arguments are converted to strings using
	 *                 {@link String#valueOf(Object)}. Arguments can be null.
	 */
	@VisibleForTesting
	static String format(String template,
	                     @Nullable Object... args)
	{
		template = String.valueOf(template); // null -> "null"

		// start substituting the arguments into the '%s' placeholders
		StringBuilder builder = new StringBuilder(
				template.length() + 16 * args.length);
		int templateStart = 0;
		int i = 0;
		while (i < args.length)
		{
			int placeholderStart = template.indexOf("%s", templateStart);
			if (placeholderStart == -1)
			{
				break;
			}
			builder.append(template.substring(templateStart, placeholderStart));
			builder.append(args[i++]);
			templateStart = placeholderStart + 2;
		}
		builder.append(template.substring(templateStart));

		// if we run out of placeholders, append the extra args in square braces
		if (i < args.length)
		{
			builder.append(" [");
			builder.append(args[i++]);
			while (i < args.length)
			{
				builder.append(", ");
				builder.append(args[i++]);
			}
			builder.append(']');
		}

		return builder.toString();
	}


}