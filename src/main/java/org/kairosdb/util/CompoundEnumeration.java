package org.kairosdb.util;

import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 Created by bhawkins on 3/27/15.
 */
public class CompoundEnumeration<E> implements Enumeration<E>
{
	private Enumeration<E>[] enums;
	private int index = 0;

	public CompoundEnumeration(Enumeration<E>[] var1)
	{
		this.enums = var1;
	}

	private boolean next()
	{
		while (this.index < this.enums.length)
		{
			if (this.enums[this.index] != null && this.enums[this.index].hasMoreElements())
			{
				return true;
			}

			++this.index;
		}

		return false;
	}

	public boolean hasMoreElements()
	{
		return this.next();
	}

	public E nextElement()
	{
		if (!this.next())
		{
			throw new NoSuchElementException();
		}
		else
		{
			return this.enums[this.index].nextElement();
		}
	}
}

