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
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.logging.Logger;

class DataMover implements Runnable
{
	Target target;
	Selector selector;
	Logger logger;
	List distributionAlgorithms;
	Map clients;
	Map servers;
	List newConnections;
	long clientToServerByteCount;
	long serverToClientByteCount;
	Thread thread;

	protected DataMover(Distributor distributor, Target target)
	{
		logger = distributor.getLogger();
		distributionAlgorithms = distributor.getDistributionAlgorithms();
		this.target = target;

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
		newConnections = new LinkedList();

		clientToServerByteCount = 0;
		serverToClientByteCount = 0;

		// Create a thread for ourselves and start it
		thread = new Thread(this, toString());
		thread.start();
	}

	/*
	 * Completed connections established by a distribution algorithm are
	 * handed to the cooresponding Target, which it turn registers them
	 * with us via this method.
	 */
	protected void addConnection(Connection conn)
	{
		// Add connection to a queue that will be processed later by
		// calling processNewConnections()
		synchronized (newConnections)
		{
			newConnections.add(conn);
		}

		// Wakeup the select so that the new connection queue
		// gets processed
		selector.wakeup();
	}

	/*
	 * Process new connections queued up by calls to addConnection()
	 */
	private void processNewConnections()
	{
		Iterator iter;
		Connection conn;
		SocketChannel client;
		SocketChannel server;
		SelectionKey key;

		synchronized (newConnections)
		{
			iter = newConnections.iterator();
			while(iter.hasNext())
			{
				conn = (Connection) iter.next();
				iter.remove();

				client = conn.getClient();
				server = conn.getServer();

				try
				{
					logger.finest("Setting channels to non-blocking mode");
					client.configureBlocking(false);
					server.configureBlocking(false);

					clients.put(client, server);
					servers.put(server, client);

					logger.finest("Registering channels with selector");
					key = client.register(selector, SelectionKey.OP_READ);
					conn.setClientSelectionKey(key);
					key = server.register(selector, SelectionKey.OP_READ);
					conn.setServerSelectionKey(key);
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
						logger.warning("Error closing channels: " +
							ioe.getMessage());
					}
				}
			}
		}
	}

	public void run()
	{
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
		ByteBuffer buffer;
		ByteBuffer reviewedBuffer;
		int r;

		final int bufferSize = 128 * 1024;
		buffer = ByteBuffer.allocateDirect(bufferSize);

		WHILETRUE:  while(true)
		{
			//
			// Register any new connections with the selector
			//
			processNewConnections();

			//
			// Now select for any channels that have data to be moved
			//
			r = 0;
			try
			{
				//logger.finest("Calling select");
				r = selector.select();
			}
			catch (IOException e)
			{
				// The only exceptions thrown by select seem to be the
				// occasional (fairly rare) "Interrupted system call"
				// which, from what I can tell, is safe to ignore.
				logger.warning(
					"Error when selecting for ready channel: " +
					e.getMessage());
				continue WHILETRUE;
			}

			logger.finest(
				"select reports " + r + " channels ready to read");

			// Work through the list of channels that have data to read
			keyIter = selector.selectedKeys().iterator();
			KEYITER:  while (keyIter.hasNext())
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
				else if (servers.containsKey(src))
				{
					clientToServer = false;
					dst = (SocketChannel) servers.get(src);
				}
				else
				{
					// We've been dropped from the maps because an
					// IOException occurred previously, nothing to do
					// but cancel our key and move on to the next ready
					// key.
					key.cancel();
					continue KEYITER;
				}

				try
				{
					// Loop as long as the source has data to read
					do  // while (readMore)
					{
						// Assume there won't be more data
						readMore = false;

						// Try to read data
						buffer.clear();
						r = src.read(buffer);
						logger.finest("Read " + r + " bytes from " + src);
						if (r > 0)  // Data was read
						{
							readMore = true;

							if (clientToServer)
							{
								clientToServerByteCount += r;
							}
							else
							{
								serverToClientByteCount += r;
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
							}
							else
							{
								logger.finest("Shutting down dest output");
								dstSocket.shutdownOutput();
							}

							// If both halves of the connection are now
							// closed, drop the entries from the
							// direction maps
							if (srcSocket.isClosed() && dstSocket.isClosed())
							{
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
					// in the EOF case above.  The key for the other
					// half of the connection will get cancelled the
					// next time it comes up in the select (see the else
					// block of the if/else if/else section at the start
					// of the KEYITER while loop).  We could add another
					// map to keep track of the keys, thus allowing us
					// to cancel it here, but the extra overhead doesn't
					// seem worth it.
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

	public long getClientToServerByteCount()
	{
		return clientToServerByteCount;
	}

	public long getServerToClientByteCount()
	{
		return serverToClientByteCount;
	}

	public String toString()
	{
		return getClass().getName() +
			" for " + target.getInetAddress() + ":" + target.getPort();
	}

	protected String getMemoryStats(String indent)
	{
		String stats;

		stats = indent + clients.size() + " entries in clients Map\n";
		stats += indent + servers.size() + " entries in servers Map\n";
		stats += indent +
			newConnections.size() + " entries in newConnections List\n";
		stats += indent +
			selector.keys().size() + " entries in selector key Set";

		return stats;
	}
}

