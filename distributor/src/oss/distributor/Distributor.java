/*****************************************************************************
 * $Id$
 *****************************************************************************
 * Simple load balancer
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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
//import java.util.logging.Handler;
import java.text.ParseException;
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Distributor
{
	protected static void usage()
	{
		System.err.println(
			"Usage:  java Distributor /path/to/distributor.conf");
		System.exit(1);
	}
		
	public static void main(String[] args)
	{
		Distributor d = new Distributor(args);
		d.balance();
	}

	InetAddress bindAddress;
	int port;
	boolean terminate;
	int connectionTimeout;
	int balanceAlgorithm;
	static final int ALGORITHM_ROUNDROBIN = 1;
	static final int ALGORITHM_HASH = 2;
	Map hashAlgorithmMap;
	List targetGroups;
	Logger logger;
	Object serviceTest;
	public Distributor(String args[])
	{
		// We'll need the logger to report errors while parsing the
		// config file so create it first.
		// *** The user should have some more control over the
		//     configuration of the logger
		logger = Logger.getLogger("Distributor");
		// This should stop the default handlers
		logger.setUseParentHandlers(false);
		logger.setLevel(Level.FINEST);
		ConsoleHandler ch = new ConsoleHandler();
		ch.setLevel(Level.FINEST);
		logger.addHandler(ch);

		//
		// Read the configuration file
		//

		if (args.length < 1) { usage(); }

		try
		{
			DocumentBuilder db =
				DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document configDoc = db.parse(args[0]);

			Element rootElement = configDoc.getDocumentElement();

			bindAddress = null;
			if (rootElement.getAttribute("bindaddr").equals("") ||
				rootElement.getAttribute("bindaddr").equals("0.0.0.0"))
			{
				logger.info("Using wildcard bind address");
			}
			else
			{
				bindAddress = InetAddress.getByName(
					rootElement.getAttribute("bindaddr"));
			}
			logger.fine("Bind address:  " + bindAddress);

			if (rootElement.getAttribute("port").equals(""))
			{
				logger.severe("The 'port' attribute is required");
				System.exit(1);
			}
			else
			{
				port = Integer.parseInt(rootElement.getAttribute("port"));
				logger.fine("Port:  " + port);
			}

			balanceAlgorithm = ALGORITHM_ROUNDROBIN;
			if (rootElement.getAttribute("algorithm").equals("roundrobin"))
			{
				balanceAlgorithm = ALGORITHM_ROUNDROBIN;
			}
			else if (rootElement.getAttribute("algorithm").equals("hash"))
			{
				balanceAlgorithm = ALGORITHM_HASH;
			}
			else
			{
				logger.warning("Unknown balance algorithm:  " +
					rootElement.getAttribute("algorithm"));
			}
			logger.fine("Balance algorithm:  " + balanceAlgorithm);

			terminate = false;
			if (rootElement.getAttribute("terminate_on_disable").equals("yes"))
			{
				terminate = true;
			}
			logger.fine("Terminate on disable:  " + terminate);

			connectionTimeout = 2000;
			if (rootElement.getAttribute("connection_timeout").equals(""))
			{
				logger.warning(
					"Connection timeout not specified, using default");
			}
			else
			{
				connectionTimeout = Integer.parseInt(
					rootElement.getAttribute("connection_timeout"));
			}
			logger.fine("Connection timeout:  " + connectionTimeout);

			//
			// Find the "target_group" nodes in the XML document
			//

			targetGroups = new ArrayList();

			// Use a TreeMap to temporarily hold the target groups.
			// Once we've loaded them all from the config file we'll
			// extract them into targetGroups in sorted order.
			TreeMap tgTree = new TreeMap();
			NodeList configChildren = rootElement.getChildNodes();
			for (int i=0 ; i<configChildren.getLength() ; i++)
			{
				Node configNode = configChildren.item(i);
				if (configNode.getNodeName().equals("target_group"))
				{
					// targets is a LinkedList in order to make the
					// round robin re-ordering of the list speedy
					List targets = new LinkedList();

					Element tgElement = (Element) configNode;
					int order =
						Integer.parseInt(tgElement.getAttribute("order"));

					NodeList tgChildren = tgElement.getChildNodes();
					for (int j=0 ; j<tgChildren.getLength() ; j++)
					{
						Node tgNode = tgChildren.item(j);
						if (tgNode.getNodeName().equals("target"))
						{
							Element targetElement = (Element) tgNode;
							targets.add(
								new Target(
									this,
									InetAddress.getByName(
										targetElement.getAttribute("hostname")),
									Integer.parseInt(
										targetElement.getAttribute("port")),
									terminate));
						}
					}

					if (targets.size() > 0)
					{
						tgTree.put(new Integer(order), targets);
					}
				}
			}

			// Extract target lists into targetGroups
			Iterator tgTreeIter = tgTree.values().iterator();
			while (tgTreeIter.hasNext())
			{
				targetGroups.add(tgTreeIter.next());
			}

			if (targetGroups.size() > 0)
			{
				logger.fine("Target groups:  ");
				Iterator tgIter = targetGroups.iterator();
				int tgCounter = 0;
				while (tgIter.hasNext())
				{
					logger.fine("Group " + tgCounter + ":");
					tgCounter++;
					List targets = (List) tgIter.next();
					Iterator targetIter = targets.iterator();
					while (targetIter.hasNext())
					{
						logger.fine("  " + targetIter.next());
					}
				}
			}
			else
			{
				logger.severe("At least one target group must be specified");
				System.exit(1);
			}

			//
			// Create the service test
			//
			serviceTest = null;
			// First get the service type
			String serviceType = rootElement.getAttribute("service_type");
			logger.fine("Service type:  " + serviceType);
			if (serviceType.equals(""))
			{
				logger.warning("No service test specified, none will be used");
			}
			else if (serviceType.equals("none"))
			{
				logger.fine("Configured for no service test");
			}
			else
			{
				// Then get the test parameters and class name that go with
				// that service type
				Element testParameters = null;
				String testClassName = null;
				for (int i=0 ; i<configChildren.getLength() ; i++)
				{
					Node node = configChildren.item(i);
					if (node.getNodeName().equals("test_parameters"))
					{
						Element elem = (Element) node;
						if (elem.getAttribute("service_type").
							equals(serviceType))
						{
							testParameters = elem;
							logger.fine(
								"Found test parameters element in config file");
						}
					}
					else if (node.getNodeName().equals("type_mapping"))
					{
						Element elem = (Element) node;
						if (elem.getAttribute("service_type").
							equals(serviceType))
						{
							testClassName = elem.getAttribute("class");
							logger.fine("Service test class:  " +
								testClassName);
						}
					}
				}

				if (testParameters == null)
				{
					logger.severe("Test parameters for service test not found");
					System.exit(1);
				}
				if (testClassName == null)
				{
					logger.severe("Service test class name not found");
					System.exit(1);
				}

				// Now construct the service test object
				try
				{
					Class testClass = Class.forName(testClassName);
					Class[] constructorArgumentClasses = {
						this.getClass(),
						Class.forName("org.w3c.dom.Element") };
					Constructor testClassConstructor =
						testClass.getConstructor(constructorArgumentClasses);
					Object[] constructorArguments = {
						this,
						testParameters };
					serviceTest = testClassConstructor.newInstance(
						constructorArguments);
				}
				catch (ClassNotFoundException e)
				{
					logger.severe("Service test class not found:  " +
						e.getMessage());
					System.exit(1);
				}
				catch (NoSuchMethodException e)
				{
					logger.severe(
						"Constructor in service test class not found:  " +
						e.getMessage());
					System.exit(1);
				}
				catch (InstantiationException e)
				{
					logger.severe("Service test class is abstract:  " +
						e.getMessage());
					System.exit(1);
				}
				catch (IllegalAccessException e)
				{
					logger.severe("Access to service test class " +
						"constructor prohibited:  " +
						e.getMessage());
					System.exit(1);
				}
				catch (InvocationTargetException e)
				{
					logger.severe("Service test class constructor " +
						"threw exception:  " + e.getMessage());
					System.exit(1);
				}
			}
		}
		catch (ParserConfigurationException e)
		{
			logger.severe("Error reading config file: " + e.getMessage());
			System.exit(1);
		}
		catch (SAXException e)
		{
			logger.severe("Error reading config file: " + e.getMessage());
			System.exit(1);
		}
		catch (IOException e)
		{
			logger.severe("Error reading config file: " + e.getMessage());
			System.exit(1);
		}
		catch (NumberFormatException e)
		{
			logger.severe("Error reading config file: " + e.getMessage());
			System.exit(1);
		}

		// *** We need a thread to clean this up over time,
		// otherwise it will potentially grow quite large
		hashAlgorithmMap = new HashMap();

		Controller controller = new Controller(this);
	}

	protected Logger getLogger()
	{
		return logger;
	}

	protected List getTargetGroups()
	{
		return targetGroups;
	}

	protected boolean getTerminate()
	{
		return terminate;
	}

	/*
	 * Returns a list of all of the Targets.  Useful for those who don't
	 * care about the target groups.
	 */
	protected List getTargets()
	{
		List all = new ArrayList();

		Iterator i = targetGroups.iterator();
		List tg;
		while (i.hasNext())
		{
			tg = (List) i.next();
			all.addAll(tg);
		}

		return all;
	}

	public void balance()
	{
		TargetSelector targetSelector = new TargetSelector(targetGroups);

		// Open the listening socket and wait for connections
		try
		{
			ServerSocketChannel server = ServerSocketChannel.open();
			server.socket().bind(new InetSocketAddress(bindAddress, port));

			while (true)
			{
				SocketChannel client = server.accept();

				// *** This should get handed to a thread so that
				// we don't hold up additional incoming
				// connections while selecting a target
				//selectTarget(targetGroups, client);
				// Hand the client off to another thread which will
				// select a target for them.  This frees us up to go
				// back to listening for new connections.
				// *** There should actually be a pool of threads so
				// that a down target doesn't hold everyone up
				targetSelector.addClient(client);
			}
		}
		catch (IOException e)
		{
			logger.severe("Error with server socket: " + e.getMessage());
			System.exit(1);
		}
	}

	class TargetSelector implements Runnable
	{
		List targetGroups;
		List queue;
		Thread thread;

		public TargetSelector(List targetGroups)
		{
			this.targetGroups = targetGroups;

			queue = new LinkedList();

			thread = new Thread(this);
			thread.start();
		}

		public void addClient(SocketChannel client)
		{
			synchronized (queue)
			{
				queue.add(client);
			}
			synchronized (this)
			{
				notifyAll();
			}
		}

		public void run()
		{
			Iterator i;
			SocketChannel client;

			while (true)
			{
				try
				{
					synchronized (this)
					{
						// This should go to an infinite wait once I'm
						// certain that I'm not an idiot and there
						// aren't any possible deadlocks here.  Until
						// then I'll play it safe and only wait 1 second.
						wait(1000);
					}
				}
				catch (InterruptedException e) {}

				synchronized (queue)
				{
					i = queue.iterator();
					while (i.hasNext())
					{
						client = (SocketChannel) i.next();
						i.remove();
						selectTarget(targetGroups, client);
					}
				}
			}
		}
	}

	protected void selectTarget(List targetGroups, SocketChannel client)
	{
		if (balanceAlgorithm == ALGORITHM_HASH)
		{
			logger.fine("Trying hash algorithm");

			// See if we have an existing mapping for this
			// client.  If so, and if that target is enabled,
			// try to send the client to it.
			Target target = (Target) hashAlgorithmMap.get(
				client.socket().getInetAddress());
			if (target != null)
			{
				logger.finer("  Existing mapping for " + client +
					" to " + target + ", trying that first");
				if (target.isEnabled() && tryTarget(target, client))
				{
					return;
				}
			}

			// No mapping found for this client, we just fall
			// through to the round robin algorithm
		}

		//
		// Round robin algorithm
		//
		logger.fine("Trying round robin algorithm");

		// Make sure nobody modifies targetGroups while we're iterating it
		synchronized (targetGroups)
		{
			List targets;
			Target target;
			int tgCounter = 0;

			// Iterate over the target groups
			Iterator tgIter = targetGroups.iterator();
			while (tgIter.hasNext())
			{
				logger.finer("Trying target group " + tgCounter);
				tgCounter++;

				targets = (List) tgIter.next();

				// Iterate over the targets in this target group
				for (int i=0 ; i<targets.size() ; i++)
				{
					// Make sure the following two operations are
					// sequential, just to be safe.
					synchronized (targets)
					{
						// Take the first target off the list
						target = (Target) targets.remove(0);

						// Put the target back on the list at the end so that
						// we cycle through all of the targets over time, thus
						// the round robin.
						targets.add(target);
					}

					if (tryTarget(target, client))
					{
						if (balanceAlgorithm == ALGORITHM_HASH)
						{
							// Store a mapping for this client
							// so that future connections from
							// them can get sent to the same
							// target.
							hashAlgorithmMap.put(
								client.socket().getInetAddress(), target);
						}

						return;
					}
				}
			}
		}

		// If we get here, it means that we were unable to find a
		// working target for the client.  Disconnect them.
		logger.warning("Unable to find a working target for client");
		try { client.close(); } catch (IOException e) {}
	}

	protected boolean tryTarget(Target target, SocketChannel client)
	{
		logger.finer("  Trying " + target);

		try
		{
			synchronized (target)
			{
				if (target.isEnabled())
				{
					SocketChannel server = SocketChannel.open();
					server.socket().connect(
						new InetSocketAddress(
							target.getInetAddress(),
							target.getPort()),
						connectionTimeout);
					Connection conn = new Connection(client, server);
					target.addConnection(conn);
					logger.finer("  Connection to " + target + " successful");
					return true;
				}
				else
				{
					return false;
				}
			}
		}
		catch (IOException e)
		{
			logger.warning("Error connecting to target: " + e.getMessage());
			return false;
		}
	}

	/*
	 * Parse log level names into Level constants.
	 * i.e. take "warning" and return Level.WARNING.
	 */
	public static Level parseLogLevel(String levelName)
		throws ParseException
	{
		if (levelName.equals("off"))
		{
			return Level.OFF;
		}
		else if (levelName.equals("severe"))
		{
			return Level.SEVERE;
		}
		else if (levelName.equals("warning"))
		{
			return Level.WARNING;
		}
		else if (levelName.equals("info"))
		{
			return Level.INFO;
		}
		else if (levelName.equals("config"))
		{
			return Level.CONFIG;
		}
		else if (levelName.equals("fine"))
		{
			return Level.FINE;
		}
		else if (levelName.equals("finer"))
		{
			return Level.FINER;
		}
		else if (levelName.equals("finest"))
		{
			return Level.FINEST;
		}
		else if (levelName.equals("all"))
		{
			return Level.ALL;
		}
		else
		{
			throw new ParseException("Unrecognized log level", 0);
		}
	}
}

