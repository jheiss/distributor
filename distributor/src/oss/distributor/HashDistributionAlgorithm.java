/*****************************************************************************
 * $Id$
 *****************************************************************************
 * Distribution algorithm based on the client's IP address.  If
 * possible, repeat connections coming from a given IP address are sent
 * back to the same server.
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;
import java.net.InetAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.w3c.dom.Element;

class HashDistributionAlgorithm
	extends DistributionAlgorithm implements Runnable
{
	int hashTimeout;
	Map ipMap;
	Map lastConnectTime;
	Selector selector;
	Thread thread;

	/*
	 * Because the distribution algorithms are instantiated via
	 * Class.forName(), they must have public constructors.
	 */
	public HashDistributionAlgorithm(
		Distributor distributor, Element configElement)
	{
		super(distributor);

		/*
		 * hashTimeout defines how long we keep a record of the last
		 * target a given client was sent too.  Set it too long and
		 * you'll use a lot of memory on a busy server.  But it needs to
		 * be long enough that client sessions get sent to the right
		 * server.  The right value is highly dependant on your
		 * environment.  Reasonable values probably range anywhere from
		 * one hour to a couple of days.
	 	 */
		hashTimeout = 1800000;
		if (configElement.getAttribute("hash_timeout").equals(""))
		{
			logger.warning("No hash timeout specified, using default");
		}
		else
		{
			try
			{
				hashTimeout =
					Integer.parseInt(
						configElement.getAttribute("hash_timeout"));
			}
			catch (NumberFormatException e)
			{
				logger.warning("Invalid hash timeout, using default:  " +
					e.getMessage());
			}
		}
		logger.config("Hash timeout:  " + hashTimeout);

		ipMap = new HashMap();
		lastConnectTime = new HashMap();

		thread = new Thread(this, getClass().getName());
	}

	/*
	 * This allows Distributor to delay some of our initialization until
	 * it is ready.  There are some things we need that Distributor may
	 * not have ready at the point at which it constructs us, so we wait
	 * and retrieve them at the start of run().
	 */
	public void startThread()
	{
		thread.start();
	}

	public void tryToConnect(SocketChannel client)
	{
		// See if we have an existing mapping for this client.  If so,
		// and if that target is enabled, try to send the client to it.
		Target target = (Target) ipMap.get(
			client.socket().getInetAddress());
		if (target != null)
		{
			logger.finer(
				"Existing mapping for " +
				client.socket().getInetAddress() +
				" to " + target);
			if (target.isEnabled())
			{
				initiateConnection(client, target);
			}
			else
			{
				logger.finer(
					"Existing mapping for " +
					client.socket().getInetAddress() +
					" points to a disabled target");
				// Give the client back to TargetSelector so it can try
				// another distribution algorithm
				targetSelector.addUnconnectedClient(client);
			}
		}
		else
		{
			logger.finer(
				"No existing mapping for " +
				client.socket().getInetAddress());
			// Give the client back to TargetSelector so it can try
			// another distribution algorithm
			targetSelector.addUnconnectedClient(client);
		}
	}

	public void run()
	{
		List completed;
		List failed;
		Iterator iter;
		Connection conn;
		SocketChannel client;
		InetAddress addr;
		long lastConnect;

		// Finish any initialization that was delayed until Distributor
		// was ready.
		finishInitialization();

		while (true)
		{
			// Both of the checkFor*Connections() methods return lists
			// we don't have to worry about synchronizing

			// checkForCompletedConnections blocks in a select (with a
			// timeout), which keeps us from going into a tight loop.
			completed = checkForCompletedConnections();
			iter = completed.iterator();
			while(iter.hasNext())
			{
				conn = (Connection) iter.next();
				targetSelector.addFinishedClient(conn);
			}

			failed = checkForFailedConnections();
			iter = failed.iterator();
			while(iter.hasNext())
			{
				client = (SocketChannel) iter.next();
				targetSelector.addUnconnectedClient(client);
			}

			// Purge old entries from ipMap
			synchronized(lastConnectTime)
			{
				iter = lastConnectTime.entrySet().iterator();
				while(iter.hasNext())
				{
					Entry timeEntry = (Entry) iter.next();
					addr = (InetAddress) timeEntry.getKey();
					lastConnect = ((Long) timeEntry.getValue()).longValue();

					if (lastConnect + hashTimeout < System.currentTimeMillis())
					{
						synchronized(ipMap)
						{
							ipMap.remove(addr);
						}
						lastConnectTime.remove(addr);
					}
				}
			}
		}
	}

	public void connectionNotify(Connection conn)
	{
		// Store a mapping for this client's IP address so that future
		// connections from that IP can get sent to the same target.
		synchronized(ipMap)
		{
			logger.finer(
				"Storing mapping from " +
				conn.getClient().socket().getInetAddress() +
				" to " + conn.getTarget());
			ipMap.put(
				conn.getClient().socket().getInetAddress(),
				conn.getTarget());
		}
		// And record the time to allow us to dump old entries from the
		// maps after a while (see the run method).
		synchronized(lastConnectTime)
		{
			lastConnectTime.put(
				conn.getClient().socket().getInetAddress(),
				new Long(System.currentTimeMillis()));
		}
	}
}

