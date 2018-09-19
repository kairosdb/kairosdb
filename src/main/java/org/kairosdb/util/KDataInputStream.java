package org.kairosdb.util;

import java.io.DataInputStream;
import java.io.InputStream;

public class KDataInputStream extends DataInputStream implements KDataInput
{
	/**
	 Creates a DataInputStream that uses the specified
	 underlying InputStream.
	 */
	public KDataInputStream(InputStream in)
	{
		super(in);
	}
}
