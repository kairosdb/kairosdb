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
package org.kairosdb.core.telnet;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class WordSplitterTest
{
	@Test
	public void test()
	{
		assertArrayEquals(new String[]{"simple", "white", "space", "string"},
		                  WordSplitter.splitString("simple white space string"));
		assertArrayEquals(new String[]{"uses", "tab", "separators"},
		                  WordSplitter.splitString("uses\ttab\tseparators"));
		assertArrayEquals(new String[]{},
		                  WordSplitter.splitString(""));  // empty string
		assertArrayEquals(new String[]{},
		                  WordSplitter.splitString("  \t  \t"));  // only spaces
		assertArrayEquals(new String[]{"multiple", "interstitial", "space", "chars"},
		                  WordSplitter.splitString("multiple \tinterstitial\t space \t chars"));
		assertArrayEquals(new String[]{"space", "at", "start"},
		                  WordSplitter.splitString("  space at start"));
		assertArrayEquals(new String[]{"space", "at", "end"},
		                  WordSplitter.splitString("space at end  "));
		assertArrayEquals(new String[]{"space", "at", "start", "and", "end"},
		                  WordSplitter.splitString("  space at start and end  "));
		assertArrayEquals(new String[]{"nospace"},
		                  WordSplitter.splitString("nospace"));
	}

	/*@Test
	public void speedTest()
	{
		long start = System.currentTimeMillis();

		for (int I = 0; I < 1000000; I++)
		{
			WordSplitter.splitString("I like to eat apples and bananas");
		}

		System.out.println("Time: "+(System.currentTimeMillis() - start));
	}*/
}
