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
import java.nio.channels.SocketChannel;
import org.w3c.dom.Element;

class HashDistributionAlgorithm
	extends DistributionAlgorithm implements Runnable
{
	int hashTimeout;
	Map ipMap;
	Map lastConnectTime;
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
		hashTimeout = 1800000;  // 30 minutes
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
		thread.start();
	}

	protected boolean processNewClients()
	{
		Iterator iter;
		SocketChannel client;
		boolean didSomething = false;

		synchronized (newClients)
		{
			iter = newClients.iterator();
			while(iter.hasNext())
			{
				client = (SocketChannel) iter.next();
				iter.remove();

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

				didSomething = true;
			}
		}

		return didSomething;
	}

	public void processCompletedConnections(List completedConnections)
	{
		Iterator iter;
		Connection conn;

		synchronized (completedConnections)
		{
			iter = completedConnections.iterator();
			while (iter.hasNext())
			{
				conn = (Connection) iter.next();
				iter.remove();
				targetSelector.addFinishedClient(conn);
			}
		}
	}

	public void processFailedConnections(List failedConnections)
	{
		Iterator iter;
		SocketChannel client;

		synchronized (failedConnections)
		{
			iter = failedConnections.iterator();
			while (iter.hasNext())
			{
				client = (SocketChannel) iter.next();
				iter.remove();
				targetSelector.addUnconnectedClient(client);
			}
		}
	}

	/*
	 * Slowly loop, purging expired entries from ipMap
	 */
	public void run()
	{
		Iterator iter;
		InetAddress addr;
		long lastConnect;
		long sleepTime;

		// Calculate the amount of time to sleep between loops, the
		// lesser of (hashTimeout/4) or 15 minutes.
		sleepTime = hashTimeout/4;
		if (sleepTime > (15 * 60 * 1000))
		{
			sleepTime = (15 * 60 * 1000);
		}

		while (true)
		{
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

			// Pause for a reasonable amount of time before doing it
			// again.
			try { Thread.sleep(sleepTime); }
				catch (InterruptedException e) {}
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

	public String getMemoryStats(String indent)
	{
		String stats;

		stats = super.getMemoryStats(indent) + "\n";
		stats += indent +
			ipMap.size() + " entries in ipMap Map\n";
		stats += indent +
			lastConnectTime.size() + " entries in lastConnectTime Map";

		return stats;
	}
}

