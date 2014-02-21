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
package org.kairosdb.core.telnet;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class WordSplitterTest
{
	private WordSplitter wordsplitter;

	@Test
	public void test()
	{
		assertArrayEquals(new String[]{"simple", "white", "space", "string"},
		                  wordsplitter.splitString("simple white space string"));
		assertArrayEquals(new String[]{"uses", "tab", "separators"},
		                  wordsplitter.splitString("uses\ttab\tseparators"));
		assertArrayEquals(new String[]{},
		                  wordsplitter.splitString(""));  // empty string
		assertArrayEquals(new String[]{},
		                  wordsplitter.splitString("  \t  \t"));  // only spaces
		assertArrayEquals(new String[]{"multiple", "interstitial", "space", "chars"},
		                  wordsplitter.splitString("multiple \tinterstitial\t space \t chars"));
		assertArrayEquals(new String[]{"space", "at", "start"},
		                  wordsplitter.splitString("  space at start"));
		assertArrayEquals(new String[]{"space", "at", "end"},
		                  wordsplitter.splitString("space at end  "));
		assertArrayEquals(new String[]{"space", "at", "start", "and", "end"},
		                  wordsplitter.splitString("  space at start and end  "));
		assertArrayEquals(new String[]{"nospace"},
		                  wordsplitter.splitString("nospace"));
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
