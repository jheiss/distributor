/*****************************************************************************
 * $Id$
 *****************************************************************************
 * Software load balancer
 * http://distributor.sourceforge.net/
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
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.logging.LogManager;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
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
	private static void usage()
	{
		System.err.println(
			"Usage:  java -jar /path/to/distributor-x.x.jar " +
				"/path/to/distributor.conf");
		System.exit(1);
	}
		
	public static void main(String[] args)
	{
		// Perhaps one of the trickier aspects of programming is
		// Java is how to write a well behaved daemon without being
		// OS-specific.
		//
		// For example, here's the UNIX FAQ entry on writing a daemon:
		// http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
		// Nearly all of that is impossible in Java
		//
		// We do what we can here and leave the rest up to an init
		// script (distributor comes with a sample one).
		try { System.in.close(); } catch (IOException e) {}

		Distributor d = new Distributor(args);
		d.balance();
	}

	InetAddress bindAddress;
	int port;
	boolean terminateOnDisable;
	int connectionTimeout;
	int connectionFailureLimit;
	List targetGroups;
	List distributionAlgorithms;
	Logger logger;
	Object serviceTest;
	Controller controller;
	TargetSelector targetSelector;
	private Distributor(String args[])
	{
		//
		// Prepare the Java logging system for use
		//

		// The Java logging system normally reads its default settings
		// from <java home>/jre/lib/logging.properties.  However, we
		// don't want those defaults used.  So we go through a bit of
		// hackery to feed our own defaults to the logging system.
		String loggingDefaultsString = new String();
		// The default is XMLFormatter, which is too verbose for
		// most folks.  The SimpleFormatter is a bit easier to read.
		loggingDefaultsString +=
			"java.util.logging.FileHandler.formatter = ";
		loggingDefaultsString +=
			"java.util.logging.SimpleFormatter\n";
		// By default Java does not append to existing
		// log files, which is probably not what users expect.
		loggingDefaultsString +=
			"java.util.logging.FileHandler.append = true\n";
		ByteArrayInputStream loggingDefaultsStream =
			new ByteArrayInputStream(loggingDefaultsString.getBytes());
		try
		{
			LogManager.getLogManager().readConfiguration(loggingDefaultsStream);
		}
		catch (IOException e)
		{
			System.err.println("Error setting logging defaults:  " +
				e.getMessage());
			System.exit(1);
		}

		logger = Logger.getLogger(getClass().getName());
		// Let each handler pick its own level
		logger.setLevel(Level.ALL);

		//
		// Read the configuration file
		//
		int controlPort = 0;

		if (args.length < 1) { usage(); }

		try
		{
			DocumentBuilder db =
				DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document configDoc = db.parse(args[0]);

			Element rootElement = configDoc.getDocumentElement();

			// Read the logging configuration first so we can use the
			// logger for reporting errors with the rest of the
			// configuration.
			NodeList configChildren = rootElement.getChildNodes();
			for (int i=0 ; i<configChildren.getLength() ; i++)
			{
				Node configNode = configChildren.item(i);
				if (configNode.getNodeName().equals("log"))
				{
					Element logElement = (Element) configNode;

					if (logElement.getAttribute("type").equals("console"))
					{
						ConsoleHandler ch = new ConsoleHandler();
						Level consoleLevel = null;
						try
						{
							consoleLevel =
								parseLogLevel(logElement.getAttribute("level"));
						}
						catch (ParseException e)
						{
							System.err.println("Unknown log level");
							System.exit(1);
						}
						ch.setLevel(consoleLevel);
						logger.addHandler(ch);
					}
					else if (logElement.getAttribute("type").equals("file"))
					{
						FileHandler fh =
							new FileHandler(
								logElement.getAttribute("filename"));
						Level fileLevel = null;
						try
						{
							fileLevel =
								parseLogLevel(logElement.getAttribute("level"));
						}
						catch (ParseException e)
						{
							System.err.println("Unknown log level");
							System.exit(1);
						}
						fh.setLevel(fileLevel);
						logger.addHandler(fh);
					}
				}
			}

			// The logger is now configured and can be used

			bindAddress = null;
			if (rootElement.getAttribute("bindaddr").equals("") ||
				rootElement.getAttribute("bindaddr").equals("0.0.0.0"))
			{
				logger.config("Using wildcard bind address");
			}
			else
			{
				bindAddress = InetAddress.getByName(
					rootElement.getAttribute("bindaddr"));
			}
			logger.config("Bind address:  " + bindAddress);

			if (rootElement.getAttribute("port").equals(""))
			{
				logger.severe("The 'port' attribute is required");
				System.exit(1);
			}
			else
			{
				port = Integer.parseInt(rootElement.getAttribute("port"));
				logger.config("Port:  " + port);
			}

			terminateOnDisable = false;
			if (rootElement.getAttribute("terminate_on_disable").equals("yes"))
			{
				terminateOnDisable = true;
			}
			logger.config("Terminate on disable:  " + terminateOnDisable);

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
			logger.config("Connection timeout:  " + connectionTimeout);

			connectionFailureLimit = 5;
			if (rootElement.getAttribute("connection_failure_limit").equals(""))
			{
				logger.warning(
					"Connection failure limit not specified, using default");
			}
			else
			{
				connectionFailureLimit = Integer.parseInt(
					rootElement.getAttribute("connection_failure_limit"));
			}
			logger.config(
				"Connection failure limit:  " + connectionFailureLimit);

			if (rootElement.getAttribute("control_port").equals(""))
			{
				logger.warning(
					"No control port defined, no control server will " +
					"be started");
			}
			else
			{
				controlPort = Integer.parseInt(
					rootElement.getAttribute("control_port"));
			}
			logger.config("Control port:  " + controlPort);

			//
			// Read the distribution algorithm configuration and create
			// the algorithm objects
			//

			distributionAlgorithms = new ArrayList();

			// Read in the algorithm name -> class name mappings
			HashMap algoClasses = new HashMap();
			for (int i=0 ; i<configChildren.getLength() ; i++)
			{
				Node configNode = configChildren.item(i);
				if (configNode.getNodeName().equals("algo_mapping"))
				{
					Element mapElement = (Element) configNode;
					String algoName = mapElement.getAttribute("name");
					String algoClass = mapElement.getAttribute("class");
					algoClasses.put(algoName, algoClass);
				}
			}

			// Find the "algorithms" node in the XML document
			for (int i=0 ; i<configChildren.getLength() ; i++)
			{
				Node configNode = configChildren.item(i);
				if (configNode.getNodeName().equals("algorithms"))
				{
					Element algosElement = (Element) configNode;

					NodeList algosChildren = algosElement.getChildNodes();
					for (int j=0 ; j<algosChildren.getLength() ; j++)
					{
						Node algoNode = algosChildren.item(j);
						if (algoNode.getNodeName().equals("algorithm"))
						{
							Element algoElement = (Element) algoNode;
							String algoName =
								algoElement.getAttribute("name");
							Object distAlgo =
								constructObjectFromName(
									(String) algoClasses.get(algoName),
									algoElement);
							distributionAlgorithms.add(distAlgo);
						}
					}
				}
			}

			// Log the distribution algorithm configuration
			if (distributionAlgorithms.size() > 0)
			{
				logger.config("Distribution algorithms:");
				Iterator iter = distributionAlgorithms.iterator();
				while(iter.hasNext())
				{
					logger.config(iter.next().toString());
				}
			}
			else
			{
				logger.severe(
					"At least one distribution algorithm must be specified");
				System.exit(1);
			}

			//
			// Read the target group configuration
			//

			targetGroups = new ArrayList();

			// Find the "target_group" nodes in the XML document
			for (int i=0 ; i<configChildren.getLength() ; i++)
			{
				Node configNode = configChildren.item(i);
				if (configNode.getNodeName().equals("target_group"))
				{
					// targets is a LinkedList in order to make the
					// re-ordering of the list by the round robin
					// algorithm speedy
					List targets = new LinkedList();

					Element tgElement = (Element) configNode;

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
									connectionFailureLimit,
									terminateOnDisable));
						}
					}

					if (targets.size() > 0)
					{
						targetGroups.add(targets);
					}
				}
			}

			// Log the target group configuration
			if (targetGroups.size() > 0)
			{
				logger.config("Target groups:");
				Iterator tgIter = targetGroups.iterator();
				int tgCounter = 0;
				while (tgIter.hasNext())
				{
					logger.config("Group " + tgCounter + ":");
					tgCounter++;
					List targets = (List) tgIter.next();
					Iterator targetIter = targets.iterator();
					while (targetIter.hasNext())
					{
						logger.config("  " + targetIter.next());
					}
				}
			}
			else
			{
				logger.severe("At least one target group must be specified");
				System.exit(1);
			}

			//
			// Read the service test configuration and create the
			// service test object
			//

			serviceTest = null;
			// First get the service type
			String serviceType = rootElement.getAttribute("service_type");
			logger.config("Service type:  " + serviceType);
			if (serviceType.equals(""))
			{
				logger.warning("No service test specified, none will be used");
			}
			else if (serviceType.equals("none"))
			{
				logger.config("Configured for no service test");
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
						}
					}
					else if (node.getNodeName().equals("type_mapping"))
					{
						Element elem = (Element) node;
						if (elem.getAttribute("service_type").
							equals(serviceType))
						{
							testClassName = elem.getAttribute("class");
							logger.config("Service test class:  " +
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
				serviceTest = constructObjectFromName(
					testClassName, testParameters);
			}
		}
		catch (ParserConfigurationException e)
		{
			System.err.println("Error reading config file: " + e.getMessage());
			System.exit(1);
		}
		catch (SAXException e)
		{
			System.err.println("Error reading config file: " + e.getMessage());
			System.exit(1);
		}
		catch (IOException e)
		{
			System.err.println("Error reading config file: " + e.getMessage());
			System.exit(1);
		}
		catch (NumberFormatException e)
		{
			System.err.println("Error reading config file: " + e.getMessage());
			System.exit(1);
		}

		if (controlPort != 0)
		{
			controller = new Controller(this, controlPort);
		}

		targetSelector = new TargetSelector(this);

		// Start all of the threads which require delayed initialization
		targetSelector.startThread();
		Iterator iter = distributionAlgorithms.iterator();
		while(iter.hasNext())
		{
			DistributionAlgorithm algo = (DistributionAlgorithm) iter.next();
			algo.startThread();
		}
	}

	private Object constructObjectFromName(
		String className, Element configElement)
	{
		Object obj;
		try
		{
			Class objClass = Class.forName(className);
			Class[] constructorArgumentClasses = {
				this.getClass(),
				Class.forName("org.w3c.dom.Element") };
			Constructor classConstructor =
				objClass.getConstructor(constructorArgumentClasses);
			Object[] constructorArguments = {
				this,
				configElement };
			obj = classConstructor.newInstance(constructorArguments);
			return obj;
		}
		catch (ClassNotFoundException e)
		{
			logger.severe("Class not found:  " + e.getMessage());
			System.exit(1);
		}
		catch (NoSuchMethodException e)
		{
			logger.severe(
				"Constructor in class not found:  " + e.getMessage());
			System.exit(1);
		}
		catch (InstantiationException e)
		{
			logger.severe("Class is abstract:  " + e.getMessage());
			System.exit(1);
		}
		catch (IllegalAccessException e)
		{
			logger.severe(
				"Access to class constructor prohibited:  " +
				e.getMessage());
			System.exit(1);
		}
		catch (InvocationTargetException e)
		{
			logger.severe(
				"Class constructor threw exception:  " + e.getMessage());
			System.exit(1);
		}

		return null;
	}

	public Logger getLogger()
	{
		return logger;
	}

	public List getDistributionAlgorithms()
	{
		return distributionAlgorithms;
	}

	public List getTargetGroups()
	{
		return targetGroups;
	}

	public TargetSelector getTargetSelector()
	{
		return targetSelector;
	}

	public int getConnectionTimeout()
	{
		return connectionTimeout;
	}

	public int getConnectionFailureLimit()
	{
		return connectionFailureLimit;
	}

	public boolean getTerminate()
	{
		return terminateOnDisable;
	}

	/*
	 * Returns a list of all of the Targets.  Useful for those who don't
	 * care about the target groups.
	 */
	public List getTargets()
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

	private void balance()
	{
		// Open the listening socket and wait for connections
		try
		{
			ServerSocketChannel server = ServerSocketChannel.open();
			server.socket().bind(new InetSocketAddress(bindAddress, port));

			while (true)
			{
				SocketChannel client = server.accept();

				logger.fine("Accepted connection from " + client);

				// Hand the client off to another thread which will
				// select a target for them.  This frees us up to go
				// back to listening for new connections.
				targetSelector.addNewClient(client);
			}
		}
		catch (IOException e)
		{
			logger.severe("Error with server socket: " + e.getMessage());
			System.exit(1);
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

