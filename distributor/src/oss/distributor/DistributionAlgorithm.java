/*****************************************************************************
 * $Id$
 *****************************************************************************
 * Base class for distribution algorithms:  algorithms for distributing
 * connections to the backend servers.
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

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.logging.Logger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.net.InetSocketAddress;

public abstract class DistributionAlgorithm
{
	Distributor distributor;
	Logger logger;
	int connectionTimeout;
	TargetSelector targetSelector;
	Selector selector;
	Map connections;  // Map client SocketChannel -> Connection
	Map connectStartTime;  // Map client SocketChannel -> Long
	List failedConnections;

	public DistributionAlgorithm(Distributor distributor)
	{
		this.distributor = distributor;

		// We can safely do this now instead of waiting for
		// finishInitialization() because we know it's one of the first
		// things Distributor does.  Some of our child constructors may
		// want to log things so we don't want to wait.
		logger = distributor.getLogger();
		//logger = Logger.getLogger(getClass().getName());

		connections = new HashMap();
		connectStartTime = new HashMap();
		failedConnections = new LinkedList();

		try
		{
			selector = Selector.open();
		}
		catch (IOException e)
		{
			logger.severe("Error creating selector: " + e.getMessage());
			System.exit(1);
		}
	}

	public abstract void startThread();

	public void finishInitialization()
	{
		connectionTimeout = distributor.getConnectionTimeout();
		targetSelector = distributor.getTargetSelector();
	}

	/*
	 * TargetSelector uses this method to give us a client.
	 */
	public abstract void tryToConnect(SocketChannel client);

	/*
	 * Once an algorithm has picked a possible Target for a client, it
	 * uses this method to initiate a connection to that target.
	 */
	public void initiateConnection(SocketChannel client, Target target)
	{
		try
		{
			SocketChannel server = SocketChannel.open();
			server.configureBlocking(false);

			// Initiate connection
			server.connect(
				new InetSocketAddress(
					target.getInetAddress(),
					target.getPort()));

			synchronized(connections)
			{
				connections.put(
					client,
					new Connection(client, server, target));
			}

			// Record the time that the connection was initiated for later
			// use in determining if it has timed out.
			synchronized(connectStartTime)
			{
				connectStartTime.put(
					client,
					new Long(System.currentTimeMillis()));
			}

			// Register with selector
			// This action is sychronized because if the selector is
			// blocked in a select, the register call will hang until
			// that ceases to be the case.  We wakeup the selector
			// before calling register so it won't block us, but without
			// the synchronization there is no guarantee that the
			// selector won't get back to the next select call before we
			// get a chance to register.  So after select is woken up,
			// it synchronizes on 'this' in order to give us a chance to
			// complete the register call.
			synchronized(this)
			{
				// Wakeup the select so that it doesn't block us from
				// registering the channels
				selector.wakeup();

				// Use the client as the attachment since all of our
				// HashMaps use the client as the key and we'll need that
				// later to lookup this connection.
				server.register(selector, SelectionKey.OP_CONNECT, client);
			}
		}
		catch (IOException e)
		{
			logger.warning(
				"Error initiating connection to target: " +
				e.getMessage());
			synchronized(failedConnections)
			{
				failedConnections.add(client);
			}
		}
	}

	/*
	 * Deal with any connections which have completed, and return a list
	 * of those that have.
	 */
	public List checkForCompletedConnections()
	{
		int r;
		Set readyKeys;
		Iterator keyIter;
		SelectionKey key;
		SocketChannel client;
		SocketChannel server;
		List completed = new LinkedList();

		// Select for a limited amount of time so that users of this
		// method also get a chance to detect failed and timed out
		// connections.  (The run methods in individual various children
		// of this class generally loop calling this method followed by
		// the checkForFailedConnections() method.)
		r = 0;
		try
		{
			r = selector.select(connectionTimeout/2);
		}
		catch (IOException e)
		{
			// What's it mean to get an I/O exception from select?
			// Is it bad enough that we should return or exit?
			logger.warning(
				"Error when selecting for ready channel: " +
				e.getMessage());
		}

		// If someone is in the process of adding a new channel to our
		// selector, wait for them to finish.  See the comments in
		// initiateConnection for a more complete explanation.
		synchronized (this)
		{
			// Do we need anything in here to keep the compiler from
			// optimizing this block away?
			//logger.finest("checkForCompletedConnections has monitor");
		}

		if (r > 0)
		{
			logger.finest(
				"select reports " + r + " channels ready to connect");

			// Work through the list of channels that are ready
			readyKeys = selector.selectedKeys();
			keyIter = readyKeys.iterator();
			while (keyIter.hasNext())
			{
				key = (SelectionKey) keyIter.next();
				keyIter.remove();

				server = (SocketChannel) key.channel();
				client = (SocketChannel) key.attachment();

				try
				{
					server.finishConnect();
					logger.fine(
						"Connection from " + client +
						" to " + server + " complete");
					synchronized(connections)
					{
						completed.add(connections.get(client));
						connections.remove(client);
					}
					synchronized(connectStartTime)
					{
						connectStartTime.remove(client);
					}
					key.cancel();
				}
				catch (IOException e)
				{
					logger.warning("Error finishing connection");
					key.cancel();
					try
					{
						server.close();
					}
					catch (IOException ioe)
					{
						logger.warning(
							"Error closing channel: " + ioe.getMessage());
					}

					synchronized(connections)
					{
						connections.remove(client);
					}
					synchronized(connectStartTime)
					{
						connectStartTime.remove(client);
					}
					synchronized(failedConnections)
					{
						failedConnections.add(client);
					}
				}
			}
		}

		return completed;
	}

	/*
	 * Return a list of connections which have timed out or otherwise
	 * failed.
	 */
	public List checkForFailedConnections()
	{
		// Add connections which have timed out to the list of
		// connections that have failed for other reasons.
		synchronized(connectStartTime)
		{
			Entry timeEntry;
			SocketChannel client;
			long startTime;

			Iterator iter = connectStartTime.entrySet().iterator();
			while(iter.hasNext())
			{
				timeEntry = (Entry) iter.next();
				client = (SocketChannel) timeEntry.getKey();
				startTime = ((Long) timeEntry.getValue()).longValue();

				if (startTime + connectionTimeout <
					System.currentTimeMillis())
				{
					synchronized(connections)
					{
						connections.remove(client);
					}
					// Already sychronized on connectStartTime
					connectStartTime.remove(client);

					synchronized(failedConnections)
					{
						failedConnections.add(client);
					}
				}
			}
		}

		// To be consistent with checkForCompletedConnections(), return
		// a list that the caller doesn't have to worry about
		// synchronizing or emptying when done.
		List returnList = failedConnections;
		failedConnections = new LinkedList();
		return returnList;
	}

	/*
	 * Provide a default no-op implementation for this method since
	 * most algorithms don't care
	 */
	public void connectionNotify(Connection conn)
	{
		// no-op
	}

	/*
	 * Provide default no-op implementations for these methods since
	 * most algorithms won't need to do anything with the data
	 */
	public ByteBuffer reviewClientToServerData(
		SocketChannel client, SocketChannel server, ByteBuffer buffer)
	{
		return buffer;
	}
	public ByteBuffer reviewServerToClientData(
		SocketChannel server, SocketChannel client, ByteBuffer buffer)
	{
		return buffer;
	}

	public String toString()
	{
		return(
			getClass().getName() +
			" with " + connections.size() + " pending connections");
	}
}

