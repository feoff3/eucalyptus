/*************************************************************************
 * Copyright 2009-2013 Eucalyptus Systems, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 *
 * Please contact Eucalyptus Systems, Inc., 6755 Hollister Ave., Goleta
 * CA 93117, USA or visit http://www.eucalyptus.com/licenses/ if you need
 * additional information or have any questions.
 ************************************************************************/

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
