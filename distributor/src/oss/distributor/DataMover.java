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
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.logging.Logger;

class DataMover implements Runnable
{
	Selector selector;
	Logger logger;
	List distributionAlgorithms;
	Map clients;
	Map servers;
	Thread thread;

	protected DataMover(Distributor distributor)
	{
		logger = distributor.getLogger();
		distributionAlgorithms = distributor.getDistributionAlgorithms();

		try
		{
			//logger.finer("Opening selector");
			selector = Selector.open();
		}
		catch (IOException e)
		{
			logger.severe("Error creating selector: " + e.getMessage());
			System.exit(1);
		}

		clients = new HashMap();
		servers = new HashMap();

		// Create a thread for ourselves and start it
		thread = new Thread(this, getClass().getName());
		thread.start();
	}

	protected void addConnection(Connection conn)
	{
		SocketChannel client = conn.getClient();
		SocketChannel server = conn.getServer();

		try
		{
			logger.finest("Setting channels to non-blocking mode");
			client.configureBlocking(false);
			server.configureBlocking(false);

			clients.put(client, server);
			servers.put(server, client);

			synchronized (this)
			{
				// Wakeup the select so that it doesn't block us from
				// registering the channels
				selector.wakeup();

				SelectionKey key;

				logger.finest("Registering channels with selector");
				key = client.register(selector, SelectionKey.OP_READ);
				conn.setClientSelectionKey(key);
				//logger.finest("Client registered");
				key = server.register(selector, SelectionKey.OP_READ);
				conn.setServerSelectionKey(key);
				//logger.finest("Server registered");
			}
		}
		catch (IOException e)
		{
			logger.warning(
				"Error setting channels to non-blocking mode: " +
				e.getMessage());
			try
			{
				logger.fine("Closing channels");
				client.close();
				server.close();
			}
			catch (IOException ioe)
			{
				logger.warning("Error closing channels: " + ioe.getMessage());
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
		boolean clientToServer;
		Socket srcSocket;
		Socket dstSocket;
		boolean readMore;
		Iterator algoIter;
		DistributionAlgorithm algo;
		final int bufferSize = 128 * 1024;
		ByteBuffer buffer;
		ByteBuffer reviewedBuffer;
		int r;

		buffer = ByteBuffer.allocateDirect(bufferSize);

		WHILETRUE:  while(true)
		{
			r = 0;
			try
			{
				//logger.finest("Calling select");
				r = selector.select();
			}
			catch (IOException e)
			{
				// What's it mean to get an I/O exception from select?
				// Is it bad enough that we should return or exit?
				logger.warning(
					"Error when selecting for ready channel: " +
					e.getMessage());
				continue WHILETRUE;
			}

			// If someone is in the process of adding a new channel to
			// our selector (via the addConnection() method), wait for
			// them to finish
			synchronized (this)
			{
				// Do we need anything in here to keep the compiler from
				// optimizing this block away?
				//logger.finest("run has monitor");
			}

			if (r == 0)
			{
				// select was interrupted, go back to waiting
				continue WHILETRUE;
			}

			logger.finest(
				"select reports " + r + " channels ready to read");

			// Work through the list of channels that have data to read
			readyKeys = selector.selectedKeys();
			keyIter = readyKeys.iterator();
			while (keyIter.hasNext())
			{
				key = (SelectionKey) keyIter.next();
				keyIter.remove();

				// Figure out which direction this data is going and
				// get the SocketChannel that is the other half of
				// the connection.
				src = (SocketChannel) key.channel();
				if (clients.containsKey(src))
				{
					clientToServer = true;
					dst = (SocketChannel) clients.get(src);
				}
				else
				{
					clientToServer = false;
					dst = (SocketChannel) servers.get(src);
				}

				try
				{
					// Loop as long as the source has data to read
					readMore = false;  // Assume there won't be more data
					do  // while (readMore)
					{
						// Try to read data
						buffer.clear();
						r = src.read(buffer);
						logger.finest("Read " + r + " bytes from " + src);
						if (r > 0)  // Data was read
						{
							// If the buffer is full, the source will
							// likely to have more data for us to read
							if (! buffer.hasRemaining())
							{
								readMore = true;
							}

							buffer.flip();

							// Give each of the distribution algorithms a
							// chance to inspect/modify the data stream
							algoIter = distributionAlgorithms.iterator();
							reviewedBuffer = buffer;
							while (algoIter.hasNext())
							{
								algo =
									(DistributionAlgorithm) algoIter.next();
								if (clientToServer)
								{
									reviewedBuffer =
										algo.reviewClientToServerData(
											src, dst, reviewedBuffer);
								}
								else
								{
									reviewedBuffer =
										algo.reviewServerToClientData(
											src, dst, reviewedBuffer);
								}
							}

							// Send the data on to its destination
							// *** This is potentially trouble (what if
							// the destination isn't ready to accept data?)
							while (reviewedBuffer.remaining() > 0)
							{
								dst.write(reviewedBuffer);
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
								logger.finer("Closing source socket");
								srcSocket.close();
								if (clientToServer)
								{
									clients.remove(src);
								}
								else
								{
									servers.remove(src);
								}
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
								logger.finer("Closing destination socket");
								dstSocket.close();
								if (clientToServer)
								{
									servers.remove(dst);
								}
								else
								{
									clients.remove(dst);
								}
							}
							else
							{
								logger.finest("Shutting down dest output");
								dstSocket.shutdownOutput();
							}
						}
					} while (readMore);
				}
				catch (IOException e)
				{
					logger.warning(
						"Error moving data between channels: " +
						e.getMessage());

					// Cancel this key for similar reasons as given
					// in the EOF case above.
					key.cancel();

					// Drop the entries from the direction maps
					if (clientToServer)
					{
						clients.remove(src);
						servers.remove(dst);
					}
					else
					{
						clients.remove(dst);
						servers.remove(src);
					}

					try
					{
						logger.fine("Closing channels");
						src.close();
						dst.close();
					}
					catch (IOException ioe)
					{
						logger.warning(
							"Error closing channels: " + ioe.getMessage());
					}
				}
			}
		}
	}
}

