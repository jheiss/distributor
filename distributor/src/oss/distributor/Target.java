/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * A Target is a destination for traffic and generally represents a server
 * providing some sort of service like HTTP or LDAP.  A Distributor will
 * typically be configured with several Targets (otherwise it wouldn't be
 * much of a load balancer).
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

import java.net.*;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.logging.Logger;

/*
 * This class is public to allow 3rd party distribution algorithms and
 * service tests.
 */
public class Target implements Runnable
{
	Distributor distributor;
	InetAddress addr;  // Address of server
	int port;  // Port on server
	boolean enabled;  // Is this channel enabled?
	boolean terminateOnDisable;
	List connections;  // List of Connection's
	long totalConnectionCount;
	Logger logger;
	DataMover dataMover;
	Thread thread;
	// Number of consecutive failures to connect to this target
	int failureCount;
	// How many consecutive connection failures are allowed before this
	// target is disabled
	int failureCountLimit;

	protected Target(Distributor distributor,
		InetAddress addr, int port,
		int failureCountLimit, boolean terminateOnDisable)
	{
		this.distributor = distributor;
		this.addr = addr;
		this.port = port;
		this.failureCountLimit = failureCountLimit;
		this.terminateOnDisable = terminateOnDisable;

		// Use a linked list to speed up removing dead connections from
		// the middle of the list.
		connections = new LinkedList();

		logger = distributor.getLogger();

		dataMover = new DataMover(distributor, this);

		failureCount = 0;
		totalConnectionCount = 0;
		enabled = true;

		thread = new Thread(this, toString());
		thread.start();
	}

	public InetAddress getInetAddress()
	{
		return addr;
	}

	public int getPort()
	{
		return port;
	}

	protected void addConnection(Connection conn)
	{
		synchronized (connections)
		{
			connections.add(conn);
		}
		dataMover.addConnection(conn);
		totalConnectionCount++;
	}

	public synchronized void enable()
	{
		enabled = true;
		failureCount = 0;
	}

	public synchronized void disable()
	{
		if (enabled == true)  // Don't do anything if already disabled
		{
			enabled = false;
			if (terminateOnDisable)
			{
				terminateAll();
			}
		}
	}

	public synchronized boolean isEnabled()
	{
		return enabled;
	}

	public synchronized int incrementFailureCount()
	{
		failureCount++;
		if (failureCount > failureCountLimit)
		{
			logger.warning(
				"Target has exceeded failure count threshold");
			logger.warning("Disabling:  " + this);
			disable();
		}
		return failureCount;
	}

	public synchronized void resetFailureCount()
	{
		failureCount = 0;
	}

	protected void terminateAll()
	{
		synchronized (connections)
		{
			Iterator i = connections.iterator();
			while (i.hasNext())
			{
				Connection conn = (Connection) i.next();
				logger.fine("Terminating and removing connection " + conn);
				conn.terminate();
				i.remove();
			}
		}
	}

	/*
	 * Remove connections which have terminated.
	 * Frees up memory and keeps our connection count accurate.
	 */
	public void run()
	{
		while (true)
		{
			synchronized (connections)
			{
				Iterator i = connections.iterator();
				while (i.hasNext())
				{
					Connection conn = (Connection) i.next();
					if (conn.isTerminated())
					{
						logger.finer("Removing terminated connection");
						i.remove();
					}
				}
			}

			try
			{
				Thread.sleep(5000);
			} catch (InterruptedException e) {}
		}
	}

	public String toString()
	{
		return getClass().getName() + " for " + addr + ":" + port;
	}

	protected String getStats(String indent)
	{
		String stats;

		if (enabled)
		{
			stats = indent + connections.size() + " current connections\n";
		}
		else
		{
			stats = indent + "DISABLED\n";
		}

		stats += indent + totalConnectionCount + " total connections\n";
		stats += indent + dataMover.getClientToServerByteCount() +
			" client to server bytes\n";
		stats += indent + dataMover.getServerToClientByteCount() +
			" server to client bytes";

		return stats;
	}

	protected String getMemoryStats(String indent)
	{
		String stats;

		stats = indent +
			connections.size() + " entries in connections List\n";
		stats += indent + "DataMover:\n";
		stats += dataMover.getMemoryStats(indent);

		return stats;
	}
}

