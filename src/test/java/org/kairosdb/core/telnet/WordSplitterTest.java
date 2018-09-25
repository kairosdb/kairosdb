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

import static org.assertj.core.api.Assertions.*;

public class WordSplitterTest
{
	@Test
	public void test()
	{
		assertThat(WordSplitter.splitString("simple white space string"))
				.containsExactly("simple", "white", "space", "string");
		assertThat(WordSplitter.splitString("uses\ttab\tseparators"))
				.containsExactly("uses", "tab", "separators");
		assertThat(WordSplitter.splitString("")) // empty string
				.containsExactly();
		assertThat(WordSplitter.splitString("  \t  \t"))  // only spaces
				.containsExactly();
		assertThat(WordSplitter.splitString("multiple \tinterstitial\t space \t chars"))
				.containsExactly("multiple", "interstitial", "space", "chars");
		assertThat(WordSplitter.splitString("  space at start"))
				.containsExactly("space", "at", "start");
		assertThat(WordSplitter.splitString("space at end  "))
				.containsExactly("space", "at", "end");
		assertThat(WordSplitter.splitString("  space at start and end  "))
				.containsExactly("space", "at", "start", "and", "end");
		assertThat(WordSplitter.splitString("nospace"))
				.containsExactly("nospace");

		assertThat(WordSplitter.splitString("\"quoted space\""))
				.containsExactly("quoted space");

		assertThat(WordSplitter.splitString("\"quoted space\" at the begining"))
				.containsExactly("quoted space", "at", "the", "begining");

		assertThat(WordSplitter.splitString("quoted space at \"the end\""))
				.containsExactly("quoted", "space", "at", "the end");

		assertThat(WordSplitter.splitString("quoted string \"with\"in\" it"))
				.containsExactly("quoted", "string", "with\"in", "it");
	}

	@Test
	public void speedTest()
	{
		long start = System.currentTimeMillis();

		for (int I = 0; I < 1000000; I++)
		{
			WordSplitter.splitString("I like to eat apples and bananas");
		}

		System.out.println("Time: "+(System.currentTimeMillis() - start));
	}
}
