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
	Map newConnections;
	Map pendingConnections;
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

		newConnections = new HashMap();
		pendingConnections = new HashMap();
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

	/*
 	* This allows Distributor to delay some of our initialization until
 	* it is ready.  There are some things we need that Distributor may
 	* not have ready at the point at which it constructs us.
 	*/
	public void finishInitialization()
	{
		connectionTimeout = distributor.getConnectionTimeout();
		targetSelector = distributor.getTargetSelector();
		startThread();
	}

	public abstract void startThread();

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
		// Put the information into a queue that will be processed later
		// by calling processNewConnections()
		synchronized(newConnections)
		{
			newConnections.put(client, target);
		}

		// Wakeup the select so that the new connection queue
		// gets processed
		selector.wakeup();
	}

	/*
	 * Process new connections queued up by calls to initiateConnection()
	 */
	private void processNewConnections()
	{
		Iterator iter;
		Entry newEntry;
		SocketChannel client;
		Target target;
		SocketChannel connToServer;
		SelectionKey key;

		synchronized(newConnections)
		{
			iter = newConnections.entrySet().iterator();
			while(iter.hasNext())
			{
				newEntry = (Entry) iter.next();
				iter.remove();
				client = (SocketChannel) newEntry.getKey();
				target = (Target) newEntry.getValue();

				try
				{
					connToServer = SocketChannel.open();
					connToServer.configureBlocking(false);

					// Initiate connection
					connToServer.connect(
						new InetSocketAddress(
							target.getInetAddress(),
							target.getPort()));

					// Use the client as the attachment to the key since
					// we'll need it later to lookup this connection's
					// state info in the pendingConnections map
					key = connToServer.register(
						selector, SelectionKey.OP_CONNECT, client);

					synchronized (pendingConnections)
					{
						// The target is needed later to create the
						//   Connection object if this connection succeeds
						// The time that the connection was initiated is
						//   used later in determining if this connection
						//   has timed out.
						// The selection key is needed so that it can be
						//   canceled if the connection does time out.
						pendingConnections.put(
							client,
							new PendingConnectionState(
								target, System.currentTimeMillis(), key));
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
		}
	}

	/*
	 * Deal with any connections which have completed, and return a list
	 * of those that have.
	 */
	public List checkForCompletedConnections()
	{
		int r;
		Iterator keyIter;
		SelectionKey key;
		SocketChannel client;
		SocketChannel server;
		PendingConnectionState connState;
		List completed = new LinkedList();

		processNewConnections();

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
			// The only exceptions thrown by select seem to be the
			// occasional (fairly rare) "Interrupted system call"
			// which, from what I can tell, is safe to ignore.
			logger.warning(
				"Error when selecting for ready channel: " +
				e.getMessage());
			return completed;
		}

		logger.finest(
			"select reports " + r + " channels ready to connect");

		// Work through the list of channels that are ready
		keyIter = selector.selectedKeys().iterator();
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
				synchronized(pendingConnections)
				{
					connState =
						(PendingConnectionState)
						pendingConnections.get(client);
					completed.add(
						new Connection(
							client, server, connState.getTarget()));
					pendingConnections.remove(client);
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

				synchronized(pendingConnections)
				{
					pendingConnections.remove(client);
				}
				synchronized(failedConnections)
				{
					failedConnections.add(client);
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
		synchronized(pendingConnections)
		{
			Entry pendingEntry;
			SocketChannel client;
			PendingConnectionState connState;

			Iterator iter = pendingConnections.entrySet().iterator();
			while(iter.hasNext())
			{
				pendingEntry = (Entry) iter.next();
				client = (SocketChannel) pendingEntry.getKey();
				connState = (PendingConnectionState) pendingEntry.getValue();

				if (connState.getStartTime() + connectionTimeout <
					System.currentTimeMillis())
				{
					logger.finer(
						"Pending connection from " + client +
						" to " + connState.getTarget() + " timed out");

					connState.getServerKey().cancel();
					iter.remove();

					// Add this client to the failed list
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
			" with " + pendingConnections.size() + " pending connections");
	}

	class PendingConnectionState
	{
		Target target;
		long startTime;
		SelectionKey serverConnectionKey;

		PendingConnectionState(
			Target target,
			long startTime,
			SelectionKey serverConnectionKey)
		{
			this.target = target;
			this.startTime = startTime;
			this.serverConnectionKey = serverConnectionKey;
		}

		Target getTarget() { return target; }
		long getStartTime() { return startTime; }
		SelectionKey getServerKey() { return serverConnectionKey; }
	}
}

