package org.kairosdb.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 Created by bhawkins on 12/10/13.
 */
public class BufferedDataInputStream extends DataInputStream
{
	public BufferedDataInputStream(RandomAccessFile file, long startPosition)
	{
		super(new BufferedInputStream(new WrappedInputStream(file, startPosition)));
	}

	private static class WrappedInputStream extends InputStream
	{
		private FileChannel m_file;
		private long m_position;

		public WrappedInputStream(RandomAccessFile file, long startPosition)
		{
			m_file = file.getChannel();
			m_position = startPosition;
		}

		@Override
		public int read() throws IOException
		{
			return -1;
		}

		@Override
		public int read(byte[] dest, int offset, int length) throws IOException
		{
			ByteBuffer buffer = ByteBuffer.wrap(dest, offset, length);

			int read = m_file.read(buffer, m_position);
			m_position += read;

			return (read);
		}
	}
}
