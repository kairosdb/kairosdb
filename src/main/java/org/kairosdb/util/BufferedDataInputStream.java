package org.kairosdb.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 Created by bhawkins on 12/10/13.
 */
public class BufferedDataInputStream extends DataInputStream
{
	/**
	 @param file
	 @param startPosition
	 @param size          Size of stream buffer
	 */
	public BufferedDataInputStream(RandomAccessFile file, long startPosition, int size)
	{
		super(new BufferedInputStream(new WrappedInputStream(file, startPosition), size));
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

		@Override
		public void close() throws IOException
		{
			//Nothing to do, m_file is a shared resource closed at a higher level
			m_file = null;
		}
	}
}
