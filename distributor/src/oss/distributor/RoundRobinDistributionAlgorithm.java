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
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;
import java.nio.channels.SocketChannel;
import org.w3c.dom.Element;

class RoundRobinDistributionAlgorithm
	extends DistributionAlgorithm implements Runnable
{
	Map clientStates;
	List nextTargetIndicies;
	List targetGroups;
	Thread thread;

	/*
	 * Because the distribution algorithms are instantiated via
	 * Class.forName(), they must have public constructors.
	 */
	public RoundRobinDistributionAlgorithm(
		Distributor distributor, Element configElement)
	{
		super(distributor);

		clientStates = new HashMap();
		nextTargetIndicies = new ArrayList();

		thread = new Thread(this, getClass().getName());
	}

	public void finishInitialization()
	{
		super.finishInitialization();
		targetGroups = distributor.getTargetGroups();
	}

	public void startThread()
	{
		thread.start();
	}

	protected void processNewClients()
	{
		Iterator iter;
		SocketChannel client;

		synchronized(newClients)
		{
			iter = newClients.iterator();
			while(iter.hasNext())
			{
				client = (SocketChannel) iter.next();
				iter.remove();

				synchronized(clientStates)
				{
					clientStates.put(client, new ClientState());
				}
				tryNextTarget(client);
			}
		}
	}

	private void tryNextTarget(SocketChannel client)
	{
		ClientState clientState;
		Target target;

		synchronized(clientStates)
		{
			clientState = (ClientState) clientStates.get(client);
		}

		try
		{
			target = clientState.getNextTarget();
		}
		catch (NoMoreTargetsException e)
		{
			// Give the client back to TargetSelector so it can try
			// another distribution algorithm
			logger.fine("Tried all targets for " + client +
				" without success");
			synchronized(clientStates)
			{
				clientStates.remove(client);
			}
			targetSelector.addUnconnectedClient(client);
			return;
		}

		if (target.isEnabled())
		{
			logger.finer(
				"Initiating connection from " + client +
				" to " + target);
			initiateConnection(client, target);
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
			processNewClients();

			// Both of the checkFor*Connections() methods return lists
			// we don't have to worry about synchronizing.

			// checkForCompletedConnections blocks in a select (with a
			// timeout), which keeps us from going into a tight loop.
			completed = checkForCompletedConnections();
			iter = completed.iterator();
			while(iter.hasNext())
			{
				conn = (Connection) iter.next();
				synchronized(clientStates)
				{
					clientStates.remove(conn.getClient());
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

	class NoMoreTargetsException extends Exception
	{
	}

	class NoSuchTargetGroupException extends Exception
	{
	}

	/*
	 * For the given target group, return the index of the target within
	 * the group that should be the starting point for the next client
	 * which wants to use that target group.  Then increments that index
	 * so the next client gets told to start at the next target.  This
	 * leads to the "round robin" action this algorithm is supposed to
	 * provide.
	 *
	 * Throws a NoSuchTargetGroupException if the given target group
	 * doesn't exist.
	 */
	private int getNextTargetIndex(int targetGroupIndex)
		throws NoSuchTargetGroupException
	{
		Integer indexInteger;
		int index;

		synchronized(nextTargetIndicies)
		{
			// Ensure that nextTargetIndicies has an entry for the given
			// target group, expand nextTargetIndicies if it doesn't.
			if (targetGroupIndex > nextTargetIndicies.size() - 1)
			{
				for (
					int i = nextTargetIndicies.size() ;
					i <= targetGroupIndex ;
					i++)
				{
					nextTargetIndicies.add(new Integer(0));
				}
			}


			// Pull the current index out of the nextTargetIndicies List,
			indexInteger =
				(Integer) nextTargetIndicies.get(targetGroupIndex);
			index = indexInteger.intValue();

			// Increment the counter, wrapping back to zero if
			// necessary, and stick it back in the List.
			List targetGroup;
			int nextIndex;
			synchronized(targetGroups)
			{
				try
				{
					targetGroup = (List) targetGroups.get(targetGroupIndex);
				}
				catch (IndexOutOfBoundsException e)
				{
					// This target group doesn't exist anymore (admins
					// can add/remove targets and target groups while
					// Distributor is running via the Controller).
					throw new NoSuchTargetGroupException();
				}
			}
			synchronized(targetGroup)
			{
				if (index < targetGroup.size() - 1)
				{
					nextIndex = index + 1;
				}
				else
				{
					nextIndex = 0;
				}
			}
			nextTargetIndicies.set(targetGroupIndex, new Integer(nextIndex));
		}

		return index;
	}

	/*
	 * Stores the state necessary to keep track of which targets
	 * a particular client has tried or should try next while iterating
	 * through all of the targets in a round-robin fashion.
	 *
	 * We need to keep track of the next target (i.e. the next one
	 * that should be tried) and the last one that should be tried.
	 *
	 * For example, imagine a target group with 4 targets:  0, 1, 2, 3
	 * Based on a call to getNextTargetIndex(), we're told to start at 2.
	 * So initially the next target will be 2.  The last target will
	 * be 1.  We'll try target 2, followed by 3, then 0, then 1.  If
	 * none of those succeed, we'll move on to the next target group.
	 */
	class ClientState
	{
		int currentTargetGroupIndex;
		int nextTargetIndex;
		int lastTargetIndex;

		ClientState()
		{
			currentTargetGroupIndex = 0;

			try
			{
				nextTargetIndex = getNextTargetIndex(currentTargetGroupIndex);
				if (nextTargetIndex > 0)
				{
					lastTargetIndex = nextTargetIndex - 1;
				}
				else
				{
					List currentTargetGroup;

					synchronized(targetGroups)
					{
						currentTargetGroup =
							(List) targetGroups.get(currentTargetGroupIndex);
					}

					lastTargetIndex = currentTargetGroup.size() - 1;
				}
			}
			catch (NoSuchTargetGroupException e)
			{
				nextTargetIndex = 0;
				lastTargetIndex = 0;
			}
		}

		/*
		 * Return the next target that should be tried for this client.
		 */
		Target getNextTarget() throws NoMoreTargetsException
		{
			List currentTargetGroup;
			Target nextTarget;

			synchronized(targetGroups)
			{
				try
				{
					currentTargetGroup =
						(List) targetGroups.get(currentTargetGroupIndex);
				}
				catch (IndexOutOfBoundsException e)
				{
					// Either we've checked all of the target groups or
					// someone removed some target groups via the
					// Controller.  Either way, there are no more
					// targets available for this client.
					throw new NoMoreTargetsException();
				}
			}
			synchronized(currentTargetGroup)
			{
				// Double-check that the size of the target group hasn't
				// changed, and do something appropriate if it has.
				if (nextTargetIndex > currentTargetGroup.size() - 1)
				{
					nextTargetIndex = 0;
				}
				if (lastTargetIndex > currentTargetGroup.size() - 1)
				{
					lastTargetIndex = currentTargetGroup.size() - 1;
				}

				nextTarget =
					(Target) currentTargetGroup.get(nextTargetIndex);
			}

			// Now increment nextTargetIndex, and
			// currentTargetGroupIndex if necessary
			if (nextTargetIndex == lastTargetIndex)
			{
				currentTargetGroupIndex++;
				try
				{
					nextTargetIndex =
						getNextTargetIndex(currentTargetGroupIndex);
					if (nextTargetIndex > 0)
					{
						lastTargetIndex = nextTargetIndex - 1;
					}
					else
					{
						List nextTargetGroup;
						synchronized (targetGroups)
						{
							nextTargetGroup =
								(List) targetGroups.get(
									currentTargetGroupIndex);
						}
						lastTargetIndex = nextTargetGroup.size() - 1;
					}
				}
				catch (NoSuchTargetGroupException e)
				{
					nextTargetIndex = 0;
					lastTargetIndex = 0;
				}
			}
			else
			{
				if (nextTargetIndex >= currentTargetGroup.size() - 1)
				{
					nextTargetIndex = 0;
				}
				else
				{
					nextTargetIndex++;
				}
			}

			return nextTarget;
		}
	}

	public String getMemoryStats(String indent)
	{
		String stats;

		stats = super.getMemoryStats(indent) + "\n";
		stats += indent +
			clientStates.size() + " entries in clientStates Map\n";
		stats += indent +
			nextTargetIndicies.size() + " entries in nextTargetIndicies List";

		return stats;
	}
}

