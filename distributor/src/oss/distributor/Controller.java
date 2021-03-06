/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * This class allows runtime control of the load balancer
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

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.text.ParseException;

/*
 * To add a new function to Controller, search for comments starting
 * with ADD to find places that you need to add something for your new
 * function.
 */

class Controller implements Runnable
{
	Distributor distributor;
	Logger logger;
	int port;
	List conns;
	Thread thread;
	ServerSocket controllerServer;

	protected Controller(Distributor distributor, int port)
	{
		this.distributor = distributor;
		logger = distributor.getLogger();
		this.port = port;

		// Use a linked list so we can remove dead connections in the
		// middle of the list easily.
		conns = new LinkedList();

		// Create a thread for ourselves and start it
		thread = new Thread(this, getClass().getName());
		thread.start();
	}

	public void run()
	{
		try
		{
			controllerServer = new ServerSocket();
			controllerServer.bind(new InetSocketAddress("127.0.0.1", port));
		}
		catch (IOException e)
		{
			logger.warning(
				"Error binding controller socket: " + e.getMessage());
			return;
		}

		while(true)
		{
			try
			{
				Socket socket = controllerServer.accept();
				logger.fine("Control connection from " + socket);
				ControllerConnection conn =
					new ControllerConnection(socket, distributor);
				conns.add(conn);
			}
			catch (IOException e)
			{
				logger.warning("Error accepting connection to controller: " +
					e.getMessage());
				shutdown();
				return;
			}

			removeDeadConnections();
		}
	}

	private void shutdown()
	{
		try
		{
			controllerServer.close();
		}
		catch (IOException e)
		{
			logger.warning(
				"Error closing controller socket: " + e.getMessage());
		}

		ControllerConnection c;
		Iterator i = conns.iterator();
		while (i.hasNext())
		{
			c = (ControllerConnection) i.next();
			c.close();
			i.remove();
		}
	}

	private void removeDeadConnections()
	{
		ControllerConnection c;

		Iterator i = conns.iterator();
		while (i.hasNext())
		{
			c = (ControllerConnection) i.next();
			if (c.isClosed())
			{
				logger.finest("Removing closed connection " + c);
				i.remove();
			}
		}
	}

	protected String getMemoryStats(String indent)
	{
		return indent + conns.size() + " entries in conns List";
	}
}

class ControllerConnection implements Runnable
{
	Socket socket;
	Distributor distributor;
	Logger logger;
	BufferedReader in;
	PrintWriter out;
	Thread thread;
	boolean closed = false;

	protected ControllerConnection(
		Socket socket, Distributor distributor) throws IOException
	{
		this.socket = socket;
		this.distributor = distributor;
		logger = distributor.getLogger();

		// Good thing we don't care about speed...
		in =
			new BufferedReader(
				new InputStreamReader(
					socket.getInputStream()));
		out = new PrintWriter(socket.getOutputStream(), true);

		// Create a thread for ourselves and start it
		thread = new Thread(this, getClass().getName());
		thread.start();
	}

	public void run()
	{
		try
		{
			boolean interactive = false;
			String line;
			StringTokenizer st;
			String command;
			READCMD: while(! closed)
			{
				// Print a prompt for interactive users
				if (interactive)
				{
					out.print("distributor> ");
					out.flush();
				}

				// Read a command
				line = in.readLine();

				if (line == null)
				{
					logger.fine("Control connection " + socket + " closed");
					close();
					return;
				}

				logger.finest("Read '" + line + "' from " + socket);
				
				st = new StringTokenizer(line);
				if (! st.hasMoreTokens())
				{
					// User didn't provide a command, switch to
					// iteractive mode and read another line
					interactive = true;
					continue READCMD;
				}
				command = st.nextToken();
				logger.finer("Read '" + command + "' command from " + socket);

				// ADD:  Add new function here
				if (command.equals("stats"))
				{
					stats(st);
				}
				else if (command.equals("memstats"))
				{
					memstats(st);
				}
				else if (command.equals("conns"))
				{
					connections(st);
				}
				else if (command.equals("threads"))
				{
					threads(st);
				}
				else if (command.equals("add"))
				{
					addTarget(st);
				}
				else if (command.equals("remove"))
				{
					removeTarget(st);
				}
				else if (command.equals("addgroup"))
				{
					addTargetGroup(st);
				}
				else if (command.equals("removegroup"))
				{
					removeTargetGroup(st);
				}
				else if (command.equals("disable"))
				{
					disableTarget(st);
				}
				else if (command.equals("enable"))
				{
					enableTarget(st);
				}
				else if (command.equals("loglevel"))
				{
					setLogLevel(st);
				}
				else if (command.equals("help"))
				{
					help(st);
				}
				else if (command.equals("quit"))
				{
					close();
					return;
				}
				else
				{
					out.println("Unknown command:  " + command);
					out.println("Try 'help' for a list of commands");
				}
			}
		}
		catch (IOException e)
		{
			logger.warning(
				"Error while communicating with controller client: " +
				e.getMessage());
			close();
			return;
		}
	}

	protected void help(StringTokenizer st)
	{
		// ADD:  Add new function here
		out.println("Commands:");
		out.println("stats");
		out.println("memstats");
		out.println("conns");
		out.println("threads");
		out.println("add");
		out.println("remove");
		out.println("addgroup");
		out.println("removegroup");
		out.println("disable");
		out.println("enable");
		out.println("loglevel");
		out.println("help");
		out.println("quit");
	}

	// ADD: Add a method for your function here

	/*
	 * Display statistics on the connections that Distributor has
	 * handled.
	 */
	protected void stats(StringTokenizer st)
	{
		// Display the statistics for each of the targets
		List targetGroups = distributor.getTargetGroups();
		synchronized (targetGroups)
		{
			List targets;
			Target target;
			Iterator targetIter;

			int tgCounter = 0;
			Iterator iter = targetGroups.iterator();
			while (iter.hasNext())
			{
				out.println("Target group " + tgCounter + ":");
				tgCounter++;

				targets = (List) iter.next();
				synchronized (targets)
				{
					targetIter = targets.iterator();
					while (targetIter.hasNext())
					{
						target = (Target) targetIter.next();
						out.println("  " + target);
						out.println(target.getStats("    "));
					}
				}
			}
		}
	}

	/*
	 * Display sizes of all of the long-term data structures used in
	 * Distributor.  This aids in detecting and fixing any bugs/leaks in
	 * the handling of those data structures.
	 */
	protected void memstats(StringTokenizer st)
	{
		Iterator iter;

		// Display the memory statistics for Controller
		out.println(distributor.getController() + ":");
		out.println(distributor.getController().getMemoryStats(""));
		out.println("");

		// Display the memory statistics for TargetSelector
		out.println(distributor.getTargetSelector() + ":");
		out.println(distributor.getTargetSelector().getMemoryStats(""));
		out.println("");

		// Display the memory statistics for each of the
		// distribution algorithms
		List distAlgos = distributor.getDistributionAlgorithms();
		synchronized (distAlgos)
		{
			DistributionAlgorithm algo;

			iter = distAlgos.iterator();
			while(iter.hasNext())
			{
				algo = (DistributionAlgorithm) iter.next();
				out.println(algo);
				out.println(algo.getMemoryStats("  "));
			}
		}
		out.println("");
		

		// Display the memory statistics for each of the targets
		List targetGroups = distributor.getTargetGroups();
		synchronized (targetGroups)
		{
			List targets;
			Target target;
			Iterator targetIter;

			int tgCounter = 0;
			iter = targetGroups.iterator();
			while (iter.hasNext())
			{
				out.println("Target group " + tgCounter + ":");
				tgCounter++;

				targets = (List) iter.next();
				synchronized (targets)
				{
					targetIter = targets.iterator();
					while (targetIter.hasNext())
					{
						target = (Target) targetIter.next();
						out.println("  " + target);
						out.println(target.getMemoryStats("    "));
					}
				}
			}
		}
	}

	/*
	 * Print all of the connections that Distributor is currently
	 * handling.
	 */
	protected void connections(StringTokenizer st)
	{
		List targetGroups = distributor.getTargetGroups();
		synchronized (targetGroups)
		{
			Iterator iter;
			List targets;
			Target target;
			Iterator targetIter;

			int tgCounter = 0;
			iter = targetGroups.iterator();
			while (iter.hasNext())
			{
				out.println("Target group " + tgCounter + ":");
				tgCounter++;

				targets = (List) iter.next();
				synchronized (targets)
				{
					targetIter = targets.iterator();
					while (targetIter.hasNext())
					{
						target = (Target) targetIter.next();
						out.println("  " + target);
						out.println(target.getConnectionList("    "));
					}
				}
			}
		}
	}

	/*
	 * Print a list of the threads in Distributor
	 */
	protected void threads(StringTokenizer st)
	{
		Thread[] tarray = new Thread[Thread.activeCount()];
		Thread.enumerate(tarray);
		for (int i=0 ; i<tarray.length ; i++)
		{
			out.println(tarray[i]);
		}
	}

	protected void addTarget(StringTokenizer st)
	{
		if (st.countTokens() != 3)
		{
			out.println("Usage: add <target group> <hostname> <port>");
			return;
		}

		int tgIndex;
		InetAddress addr;
		int port;
		Target newTarget;
		try
		{
			tgIndex = Integer.parseInt(st.nextToken());
			addr = InetAddress.getByName(st.nextToken());
			port = Integer.parseInt(st.nextToken());

			newTarget = new Target(
				distributor, addr, port,
				distributor.getConnectionFailureLimit(),
				distributor.getTerminate(), distributor.getHalfClose());
		}
		catch (UnknownHostException e)
		{
			out.println("Host not found:  " + e.getMessage());
			return;
		}
		catch (NumberFormatException e)
		{
			out.println("Target group and port must be integers");
			return;
		}

		List targetGroups = distributor.getTargetGroups();

		synchronized (targetGroups)
		{
			if (tgIndex < 0 || tgIndex >= targetGroups.size())
			{
				out.println("Invalid target group");
				return;
			}

			Iterator iter = targetGroups.iterator();
			int tgCounter = 0;
			while (iter.hasNext())
			{
				if (tgCounter == tgIndex)
				{
					List targets = (List) iter.next();
					targets.add(newTarget);
					out.println("New target added");
					break;
				}

				tgCounter++;
			}
		}
	}

	protected void removeTarget(StringTokenizer st)
	{
		if (st.countTokens() != 3)
		{
			out.println("Usage: remove <target group> <hostname> <port>");
			return;
		}

		int tgIndex;
		InetAddress addr;
		int port;
		try
		{
			tgIndex = Integer.parseInt(st.nextToken());
			addr = InetAddress.getByName(st.nextToken());
			port = Integer.parseInt(st.nextToken());
		}
		catch (UnknownHostException e)
		{
			out.println("Host not found:  " + e.getMessage());
			return;
		}
		catch (NumberFormatException e)
		{
			out.println("Target group and port must be integers");
			return;
		}

		List targetGroups = distributor.getTargetGroups();

		List targets = null;
		synchronized (targetGroups)
		{
			if (tgIndex < 0 || tgIndex >= targetGroups.size())
			{
				out.println("Invalid target group");
				return;
			}

			Iterator iter = targetGroups.iterator();
			int tgCounter = 0;
			while (iter.hasNext())
			{
				if (tgCounter == tgIndex)
				{
					targets = (List) iter.next();
					break;
				}
				tgCounter++;
			}
		}

		boolean targetFound = false;
		Target target = null;
		synchronized (targets)
		{
			Iterator targetIter = targets.iterator();
			while (targetIter.hasNext())
			{
				target = (Target) targetIter.next();
				if (target.getInetAddress().equals(addr) &&
					target.getPort() == port)
				{
					targetIter.remove();
					targetFound = true;
					break;
				}
			}
		}
		if (targetFound)
		{
			target.terminateAll();
			out.println("Target removed");
		}
		else
		{
			out.println("No matching target found");
		}
	}

	protected void addTargetGroup(StringTokenizer st)
	{
		if (st.countTokens() != 1)
		{
			out.println("Usage: addgroup <target group>");
			return;
		}

		int tgIndex;
		try
		{
			tgIndex = Integer.parseInt(st.nextToken());
		}
		catch (NumberFormatException e)
		{
			out.println("Target group must be an integer");
			return;
		}

		List targetGroups = distributor.getTargetGroups();

		synchronized (targetGroups)
		{
			if (tgIndex < 0 || tgIndex > targetGroups.size())
			{
				out.println("Invalid target group");
				return;
			}

			targetGroups.add(tgIndex, new LinkedList());
			out.println("New target group added at position " + tgIndex);
		}
	}

	protected void removeTargetGroup(StringTokenizer st)
	{
		if (st.countTokens() != 1)
		{
			out.println("Usage: removegroup <target group>");
			return;
		}

		int tgIndex;
		try
		{
			tgIndex = Integer.parseInt(st.nextToken());
		}
		catch (NumberFormatException e)
		{
			out.println("Target group must be an integer");
			return;
		}

		List targetGroups = distributor.getTargetGroups();

		// Remove the target group from the list of target groups
		List targets;
		synchronized (targetGroups)
		{
			if (tgIndex < 0 || tgIndex >= targetGroups.size())
			{
				out.println("Invalid target group");
				return;
			}

			targets = (List) targetGroups.remove(tgIndex);
		}

		// Shutdown the connections to each target in the target group
		synchronized (targets)
		{
			Iterator targetIter = targets.iterator();
			Target target;
			while (targetIter.hasNext())
			{
				target = (Target) targetIter.next();
				target.terminateAll();
			}
		}

		out.println("Target group at position " + tgIndex + " removed");
	}

	protected void disableTarget(StringTokenizer st)
	{
		if (st.countTokens() != 3)
		{
			out.println("Usage: disable <target group> <hostname> <port>");
			return;
		}

		int tgIndex;
		InetAddress addr;
		int port;
		try
		{
			tgIndex = Integer.parseInt(st.nextToken());
			addr = InetAddress.getByName(st.nextToken());
			port = Integer.parseInt(st.nextToken());
		}
		catch (UnknownHostException e)
		{
			out.println("Host not found:  " + e.getMessage());
			return;
		}
		catch (NumberFormatException e)
		{
			out.println("Target group and port must be integers");
			return;
		}

		List targetGroups = distributor.getTargetGroups();

		List targets = null;
		synchronized (targetGroups)
		{
			if (tgIndex < 0 || tgIndex >= targetGroups.size())
			{
				out.println("Invalid target group");
				return;
			}

			Iterator iter = targetGroups.iterator();
			int tgCounter = 0;
			while (iter.hasNext())
			{
				if (tgCounter == tgIndex)
				{
					targets = (List) iter.next();
					break;
				}
				tgCounter++;
			}
		}

		boolean targetFound = false;
		synchronized (targets)
		{
			Iterator targetIter = targets.iterator();
			Target target;
			while (targetIter.hasNext())
			{
				target = (Target) targetIter.next();
				if (target.getInetAddress().equals(addr) &&
					target.getPort() == port)
				{
					target.disable();
					targetFound = true;
					out.println("Target disabled");
					break;
				}
			}
		}
		if (!targetFound)
		{
			out.println("No matching target found");
		}
	}

	protected void enableTarget(StringTokenizer st)
	{
		if (st.countTokens() != 3)
		{
			out.println("Usage: enable <target group> <hostname> <port>");
			return;
		}

		int tgIndex;
		InetAddress addr;
		int port;
		try
		{
			tgIndex = Integer.parseInt(st.nextToken());
			addr = InetAddress.getByName(st.nextToken());
			port = Integer.parseInt(st.nextToken());
		}
		catch (UnknownHostException e)
		{
			out.println("Host not found:  " + e.getMessage());
			return;
		}
		catch (NumberFormatException e)
		{
			out.println("Target group and port must be integers");
			return;
		}

		List targetGroups = distributor.getTargetGroups();

		List targets = null;
		synchronized (targetGroups)
		{
			if (tgIndex < 0 || tgIndex >= targetGroups.size())
			{
				out.println("Invalid target group");
				return;
			}

			Iterator iter = targetGroups.iterator();
			int tgCounter = 0;
			while (iter.hasNext())
			{
				if (tgCounter == tgIndex)
				{
					targets = (List) iter.next();
					break;
				}
				tgCounter++;
			}
		}

		boolean targetFound = false;
		synchronized (targets)
		{
			Iterator targetIter = targets.iterator();
			Target target;
			while (targetIter.hasNext())
			{
				target = (Target) targetIter.next();
				if (target.getInetAddress().equals(addr) &&
					target.getPort() == port)
				{
					target.enable();
					targetFound = true;
					out.println("Target enabled");
					break;
				}
			}
		}
		if (!targetFound)
		{
			out.println("No matching target found");
		}
	}

	protected void setLogLevel(StringTokenizer st)
	{
		if (st.countTokens() != 1)
		{
			out.println("Usage: loglevel " +
				"off|severe|warning|info|config|fine|finer|finest|all");
			return;
		}

		Level newLevel;
		String levelName = st.nextToken();
		try
		{
			newLevel = Distributor.parseLogLevel(levelName);
		}
		catch (ParseException e)
		{
			out.println("Unrecognized log level");
			return;
		}

		logger.setLevel(newLevel);
	}

	protected boolean isClosed()
	{
		return closed;
	}

	protected void close()
	{
		try
		{
			socket.close();
		}
		catch (IOException e)
		{
			logger.warning(
				"Error closing connection to controller client: " +
				e.getMessage());
		}
		closed = true;
	}

	public String toString()
	{
		return "ControllerConnection from " + socket;
	}
}

