package com.eucalyptus.objectstorage;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;


public class ChannelBufferStreamingInputStream extends ChannelBufferInputStream {
	private ChannelBuffer b;
	
	@Override
	public boolean markSupported() {
		return super.markSupported();
	}

	private static final Logger LOG = Logger.getLogger(ChannelBufferStreamingInputStream.class); 

	@Override
	public int available() throws IOException {
		return b.readableBytes();
	}

	@Override
	public void mark(int readlimit) {
		super.mark(readlimit);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return super.read(b, off, len);
	}

	@Override
	public int readBytes() {
		return super.readBytes();
	}

	@Override
	public void reset() throws IOException {
		super.reset();
	}

	@Override
	public long skip(long n) throws IOException {
		return super.skip(n);
	}

	public ChannelBufferStreamingInputStream(ChannelBuffer buffer) {
		super(buffer);
		this.b = buffer;
	}

}
