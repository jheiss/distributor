/*****************************************************************************
 * $Id$
 *****************************************************************************
 * Distribution algorithm which cycles through all of the available
 * targets until it finds one it can connect to.
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;
import java.nio.channels.SocketChannel;
import org.w3c.dom.Element;

class RoundRobinDistributionAlgorithm
	extends DistributionAlgorithm implements Runnable
{
	Map currentTargetGroups;
	Map currentTargets;
	Thread thread;

	/*
	 * Because the distribution algorithms are instantiated via
	 * Class.forName(), they must have public constructors.
	 */
	public RoundRobinDistributionAlgorithm(
		Distributor distributor, Element configElement)
	{
		super(distributor);

		currentTargetGroups = new HashMap();
		currentTargets = new HashMap();

		thread = new Thread(this, getClass().getName());
	}

	public void startThread()
	{
		thread.start();
	}

	public void tryToConnect(SocketChannel client)
	{
		synchronized(currentTargetGroups)
		{
			currentTargetGroups.put(client, new Integer(0));
		}
		synchronized(currentTargets)
		{
			currentTargets.put(client, new Integer(0));
		}

		tryNextTarget(client);
	}

	private void tryNextTarget(SocketChannel client)
	{
		List targetGroups = distributor.getTargetGroups();
		int currentTargetGroupNumber;
		List currentTargetGroup;
		int currentTargetNumber;
		Target currentTarget;

		// Check to see if we're out of available targets for this
		// client.
		// (Flagged by the previous invocation of this method by
		// removing us from the Maps.)
		synchronized(currentTargetGroups)
		{
			if (currentTargetGroups.get(client) == null)
			{
				// Give the client back to TargetSelector so it can try
				// another distribution algorithm
				logger.fine("Tried all targets for " + client +
					" without success");
				targetSelector.addUnconnectedClient(client);
				return;
			}
		}

		synchronized(currentTargetGroups)
		{
			currentTargetGroupNumber =
				((Integer) currentTargetGroups.get(client)).intValue();
		}
		synchronized(targetGroups)
		{
			currentTargetGroup =
				(List) targetGroups.get(currentTargetGroupNumber);
		}
		synchronized(currentTargets)
		{
			currentTargetNumber =
				((Integer) currentTargets.get(client)).intValue();
		}
		synchronized(currentTargetGroup)
		{
			currentTarget =
				(Target) currentTargetGroup.get(currentTargetNumber);
		}

		// If we're trying the first target in the group, cycle the
		// group to create the round-robin action.
		if (currentTargetNumber == 0)
		{
			logger.finest("Rotating target group");
			synchronized (currentTargetGroup)
			{
				// Take the first target off the list
				Target target = (Target) currentTargetGroup.remove(0);

				// Put the target back on the list at the end so
				// that we cycle through all of the targets over
				// time, thus the round robin.
				currentTargetGroup.add(target);
			}
		}

		// Now increment the current target group/target counters so
		// we're ready for the next time if this connection fails
		//
		// If there are still targets in this target group, just
		// increment the current target number counter.
		if (currentTargetNumber < currentTargetGroup.size() - 1)
		{
			synchronized(currentTarget)
			{
				currentTargets.put(
					client,
					new Integer(currentTargetNumber + 1));
			}
		}
		// Else if there are more target groups available, increment the
		// target group counter and reset the target counter to zero.
		else if (currentTargetGroupNumber < targetGroups.size() - 1)
		{
			synchronized(currentTargetGroups)
			{
				currentTargetGroups.put(
					client,
					new Integer(currentTargetGroupNumber + 1));
			}
			synchronized(currentTargets)
			{
				currentTargets.put(client, new Integer(0));
			}
		}
		// Else we've run out of targets and target groups, drop this
		// client from the maps.
		else
		{
			synchronized(currentTargetGroups)
			{
				currentTargetGroups.remove(client);
			}
			synchronized(currentTargets)
			{
				currentTargets.remove(client);
			}
		}

		if (currentTarget.isEnabled())
		{
			logger.finer(
				"Initiating connection from " + client +
				" to " + currentTarget);
			initiateConnection(client, currentTarget);
		}
		else
		{
			tryNextTarget(client);
		}
	}

	public void run()
	{
		List completed;
		List failed;
		Iterator iter;
		Connection conn;
		SocketChannel client;

		while (true)
		{
			// Both of the checkFor*Connections() methods return lists
			// we don't have to worry about synchronizing.

			// checkForCompletedConnections blocks in a select (with a
			// timeout), which keeps us from going into a tight loop.
			completed = checkForCompletedConnections();
			iter = completed.iterator();
			while(iter.hasNext())
			{
				conn = (Connection) iter.next();
				synchronized(currentTargetGroups)
				{
					currentTargetGroups.remove(conn.getClient());
				}
				synchronized(currentTargets)
				{
					currentTargets.remove(conn.getClient());
				}

				targetSelector.addFinishedClient(conn);
			}

			failed = checkForFailedConnections();
			iter = failed.iterator();
			while(iter.hasNext())
			{
				client = (SocketChannel) iter.next();
				tryNextTarget(client);
			}
		}
	}
}

