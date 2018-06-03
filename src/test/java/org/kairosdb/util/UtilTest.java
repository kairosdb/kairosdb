package org.kairosdb.util;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class UtilTest
{
	@Test(expected = NullPointerException.class)
	public void test_isNumber_nullString_invalid()
	{
		Util.isNumber(null);
	}

	@Test
	public void test_isNumber_emptyString()
	{
		assertThat(Util.isNumber(""), equalTo(false));
	}

	@Test
	public void test_isNumber_onlyPlus()
	{
		assertThat(Util.isNumber("+"), equalTo(false));
	}

	@Test
	public void test_isNumber_onlyMinus()
	{
		assertThat(Util.isNumber("-"), equalTo(false));
	}

	@Test
	public void test_isNumber_onlyPeriod()
	{
		assertThat(Util.isNumber("."), equalTo(false));
	}

	@Test
	public void test_isNumber_trailingPeriod()
	{
		assertThat(Util.isNumber("3."), equalTo(false));
	}

	@Test
	public void test_isNumber_hasLetter()
	{
		assertThat(Util.isNumber("3.5A5"), equalTo(false));
	}

	@Test
	public void test_isNumber_withMinusSignInMiddle()
	{
		assertThat(Util.isNumber("10-2"), equalTo(false));
	}

	@Test
	public void test_isNumber_withPlusSignInMiddle()
	{
		assertThat(Util.isNumber("10+2"), equalTo(false));
	}

	@Test
	public void test_isNumber_startsWithLetter()
	{
		assertThat(Util.isNumber("s123"), equalTo(false));
	}

	@Test
	public void test_isNumber_doubleValue()
	{
		assertThat(Util.isNumber("3.55"), equalTo(true));
	}

	@Test
	public void test_isNumber_integerValue()
	{
		assertThat(Util.isNumber("102"), equalTo(true));
	}

	@Test
	public void test_isNumber_withPlusSign()
	{
		assertThat(Util.isNumber("+102"), equalTo(true));
	}

	@Test
	public void test_isNumber_withMinusSign()
	{
		assertThat(Util.isNumber("-102"), equalTo(true));
	}

	@Test
	public void test_replaceEach_emptyString()
	{
		assertEquals("", Util.replaceEach("", ImmutableMap.of("foo", "*")));
	}

	@Test
	public void test_replaceEach_emptyMap()
	{
		String text = "foo bar";
		assertEquals(text, Util.replaceEach(text, new HashMap<>()));
	}

	@Test
	public void test_replaceEach_searchValueNotInText()
	{
		String text = "foo bar";
		assertEquals(text, Util.replaceEach(text, ImmutableMap.of("baz", "$")));
	}

	@Test
	public void test_replaceEach_replaceSingleValue()
	{
		String text = "foo bar";
		assertEquals("* bar", Util.replaceEach(text, ImmutableMap.of("foo", "*")));
	}

	@Test
	public void test_replaceEach_replaceMultipleValues()
	{
		String text = "foo bar";
		assertEquals("* #", Util.replaceEach(text, ImmutableMap.of("foo", "*", "bar", "#")));
	}

	@Test
	public void test_replaceEach_replaceMultipleFoundInstances()
	{
		String text = "foo bar foo";
		assertEquals("* bar *", Util.replaceEach(text, ImmutableMap.of("foo", "*")));
	}
}
