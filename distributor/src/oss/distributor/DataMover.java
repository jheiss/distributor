/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * This class passes data back and forth from clients and servers for
 * established connections through the load balancer.
 *****************************************************************************
 * Copyright 2003 Jason Heiss
 * 
 * This file is part of Distributor.
 * 
 * Distributor is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * Distributor is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Distributor; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *****************************************************************************
 */

package oss.distributor;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.Iterator;
import java.util.logging.Logger;

public class DataMover implements Runnable
{
	Selector selector;
	Logger logger;
	Thread thread;

	public DataMover(Distributor distributor)
	{
		logger = distributor.getLogger();

		try
		{
			logger.finer("Opening selector");
			selector = Selector.open();
		}
		catch (IOException e)
		{
			logger.severe("Error creating selector: " + e.getMessage());
			System.exit(1);
		}

		// Create a thread for ourselves and start it
		thread = new Thread(this);
		thread.start();
	}

	public void addConnection(Connection conn)
	{
		SocketChannel client = conn.getClient();
		SocketChannel server = conn.getServer();

		try
		{
			logger.finest("Setting channels to non-blocking mode");
			client.configureBlocking(false);
			server.configureBlocking(false);

			// Register the two channels with the other half of the
			// pair as the attachment to the selection key.  This
			// allows to easily figure out who to transfer the data
			// to in the selection loop below.
			synchronized (this)
			{
				// Wakeup the select so that it doesn't block us from
				// registering the channels
				selector.wakeup();

				SelectionKey key;

				logger.finest("Registering channels with selector");
				key = client.register(selector, SelectionKey.OP_READ, server);
				conn.setClientSelectionKey(key);
				logger.finest("Client registered");
				key = server.register(selector, SelectionKey.OP_READ, client);
				conn.setServerSelectionKey(key);
				logger.finest("Server registered");
			}
		}
		catch (IOException e)
		{
			logger.warning(
				"Error setting channels to non-blocking mode: " +
				e.getMessage());
			try
			{
				logger.finest("Closing channels");
				client.close();
				server.close();
			}
			catch (IOException ioe)
			{
				logger.warning("Error closing channels: " + e.getMessage());
			}
		}
	}

	public void run()
	{
		Set readyKeys;
		Iterator keyIter;
		SelectionKey key;
		SocketChannel src;
		SocketChannel dst;
		Socket srcSocket;
		Socket dstSocket;
		final int bufferSize = 128 * 1024;
		ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
		int r;

		while(true)
		{
			r = 0;
			try
			{
				logger.finest("Calling select");
				r = selector.select();
			}
			catch (IOException e)
			{
				// What's it mean to get an I/O exception from select?
				// Is it bad enough that we should return or exit?
				logger.warning(
					"Error when selecting for ready channel: " +
					e.getMessage());
			}

			// If someone is in the process of adding a new channel to
			// our selector, wait for them to finish
			synchronized (this)
			{
				// Do we need anything in here to keep the compiler from
				// optimizing this block away?
				logger.finest("run has monitor");
			}

			if (r > 0)
			{
				logger.finest("select reports " + r + " channels ready");

				// Work through the list of channels that have
				// data to read
				readyKeys = selector.selectedKeys();
				keyIter = readyKeys.iterator();
				while (keyIter.hasNext())
				{
					key = (SelectionKey) keyIter.next();
					keyIter.remove();

					src = (SocketChannel) key.channel();
					dst = (SocketChannel) key.attachment();

					try
					{
						// Try to read data
						buffer.clear();
						r = src.read(buffer);
						logger.finest("Read " + r + " bytes from " + src);
						if (r > 0)  // Data was read
						{
							buffer.flip();
							while (buffer.remaining() > 0)
							{
								dst.write(buffer);
							}
						}
						else if (r == -1)  // EOF
						{
							// Cancel this key, otherwise this channel
							// will repeatedly trigger select to tell us
							// that it is at EOF.
							key.cancel();

							srcSocket = src.socket();
							dstSocket = dst.socket();

							// If the other half of the socket is
							// already shutdown then go ahead and close
							// the socket
							if (srcSocket.isOutputShutdown())
							{
								logger.finest("Closing source socket");
								srcSocket.close();
							}
							// Otherwise just close down the input
							// stream.  This allows any return traffic
							// to continue to flow.
							else
							{
								logger.finest("Shutting down source input");
								srcSocket.shutdownInput();
							}

							// Do the same thing for the destination,
							// but using the reverse streams.
							if (dstSocket.isInputShutdown())
							{
								logger.finest("Closing destination socket");
								dstSocket.close();
							}
							else
							{
								logger.finest("Shutting down dest output");
								dstSocket.shutdownOutput();
							}
						}
					}
					catch (IOException e)
					{
						logger.warning(
							"Error moving data between channels: " +
							e.getMessage());

						// Cancel this key for similar reasons as given
						// in the EOF case above.
						key.cancel();

						try
						{
							logger.finest("Closing channels");
							src.close();
							dst.close();
						}
						catch (IOException ioe)
						{
							logger.warning(
								"Error closing channels: " + e.getMessage());
						}
					}
				}
			}
		}
	}
}

