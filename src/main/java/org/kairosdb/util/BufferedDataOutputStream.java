package org.kairosdb.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 Created by bhawkins on 12/10/13.
 */
public class BufferedDataOutputStream extends DataOutputStream
{
	private WrappedOutputStream m_wrappedOutputStream;

	public static BufferedDataOutputStream create(RandomAccessFile file, long startPosition)
	{
		WrappedOutputStream outputStream = new WrappedOutputStream(file, startPosition);
		BufferedDataOutputStream ret = new BufferedDataOutputStream(outputStream);
		ret.setWrappedOutputStream(outputStream);

		return ret;
	}

	private BufferedDataOutputStream(WrappedOutputStream outputStream)
	{
		super(new BufferedOutputStream(outputStream));
	}

	private void setWrappedOutputStream(WrappedOutputStream outputStream)
	{
		m_wrappedOutputStream = outputStream;
	}

	public long getPosition()
	{
		return m_wrappedOutputStream.getPosition();
	}

	private static class WrappedOutputStream extends OutputStream
	{
		private FileChannel m_file;
		private long m_position;

		public WrappedOutputStream(RandomAccessFile file, long startPosition)
		{
			m_file = file.getChannel();
			m_position = startPosition;
		}

		public long getPosition()
		{
			return m_position;
		}

		@Override
		public void write(int b) throws IOException
		{
		}

		@Override
		public void write(byte[] src, int offset, int length) throws IOException
		{
			ByteBuffer buffer = ByteBuffer.wrap(src, offset, length);
			while (buffer.hasRemaining())
			{
				int written = m_file.write(buffer, m_position);
				m_position += written;
			}
		}
	}
}
