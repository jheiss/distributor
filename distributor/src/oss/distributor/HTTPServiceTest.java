/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * Performs service tests against an HTTP server
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.Iterator;
import java.util.logging.Logger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.MalformedURLException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HTTPServiceTest implements Runnable
{
	Distributor distributor;
	Logger logger;

	int frequency;  // How often should the test be done?
	int timeout;  // How long do we wait for the test to complete before
	              // deciding that it has failed?

	boolean useSSL;
	String path;
	String userAgent;

	public static final int REQUIREMENT_RESPONSE_CODE = 1;
	public static final int REQUIREMENT_CONTENT_TYPE = 2;
	public static final int REQUIREMENT_DOCUMENT_TEXT = 3;
	Map requirements;

	Thread thread;

	public HTTPServiceTest(Distributor distributor, Element configElement)
	{
		this.distributor = distributor;
		logger = distributor.getLogger();

		frequency = 60000;  // Default of 60s
		try
		{
			frequency =
				Integer.parseInt(configElement.getAttribute("frequency"));
		}
		catch (NumberFormatException e)
		{
			logger.warning("Invalid frequency, using default:  " +
				e.getMessage());
		}
		logger.fine("Test frequency:  " + frequency);

		timeout = 5000;  // Default of 5s
		try
		{
			timeout =
				Integer.parseInt(configElement.getAttribute("timeout"));
		}
		catch (NumberFormatException e)
		{
			logger.warning("Invalid timeout, using default:  " +
				e.getMessage());
		}
		logger.fine("Test timeout:  " + timeout);

		useSSL = false;
		if (configElement.getAttribute("use_ssl").equals("yes"))
		{
			useSSL = true;
		}
		logger.fine("Use SSL:  " + useSSL);

		userAgent = "Distributor - http://distributor.sourceforge.net/";
		if (! configElement.getAttribute("user_agent").equals(""))
		{
			userAgent = configElement.getAttribute("user_agent");
		}
		logger.fine("User agent:  " + userAgent);

		if (! configElement.getAttribute("ssl_keystore").equals(""))
		{
			System.setProperty(
				"javax.net.ssl.trustStore",
				configElement.getAttribute("ssl_keystore"));
		}
		logger.fine("SSL keystore:  " +
			System.getProperty("javax.net.ssl.trustStore"));

		// *** All of the auth related attributes are ignored for now

		// Extract document path and requirements from XML document.
		// Document structure:
		// 
		// <get path="/index.html">
		//     <response_code value="200"/>
		//     <content_type value="text/html"/>
		//     <document_text value="Welcome to XYZ Corp"/>
		// </get>
		requirements = new LinkedHashMap();
		NodeList configChildren = configElement.getChildNodes();
		for (int i=0 ; i<configChildren.getLength() ; i++)
		{
			Node configNode = configChildren.item(i);
			if (configNode.getNodeName().equals("get"))
			{
				Element getElement = (Element) configNode;
				path = getElement.getAttribute("path");
				NodeList getChildren = getElement.getChildNodes();
				for (int j=0 ; j<getChildren.getLength() ; j++)
				{
					Node getNode = getChildren.item(j);
					if (getNode.getNodeName().equals("response_code"))
					{
						Element responseCodeElement = (Element) getNode;
						int code =
							Integer.parseInt(
								responseCodeElement.getAttribute("value"));
						requirements.put(
							new Integer(REQUIREMENT_RESPONSE_CODE),
							new Integer(code));
					}
					else if (getNode.getNodeName().equals("content_type"))
					{
						Element contentTypeElement = (Element) getNode;
						String type =
							contentTypeElement.getAttribute("value");
						requirements.put(
							new Integer(REQUIREMENT_CONTENT_TYPE),
							type);
					}
					else if (getNode.getNodeName().equals("document_text"))
					{
						Element documentTextElement = (Element) getNode;
						String requiredText =
							documentTextElement.getAttribute("value");
						requirements.put(
							new Integer(REQUIREMENT_DOCUMENT_TEXT),
							requiredText);
					}
				}
			}
		}
		if (path == null)
		{
			logger.severe("A path is required");
			System.exit(1);  // ***
		}
		if (requirements.size() == 0)
		{
			// Insert a simple requirement
			requirements.put(
				new Integer(REQUIREMENT_RESPONSE_CODE),
				new Integer(HttpURLConnection.HTTP_OK));
		}
		logger.fine("Path:  " + path);
		logger.fine("Requirements:  " + requirements);

		thread = new Thread(this);
		thread.start();
	}

	public void run()
	{
		List targets;
		Iterator i;
		Target target;
		boolean result;

		while (true)
		{
			targets = distributor.getTargets();

			// distributor.getTargets() constructs a new List, puts
			// all of the Targets into it, and returns it.  As such, we
			// don't have to worry about holding everything else up by
			// synchronizing on the list for a long time (the testing
			// could take many seconds).  In fact, we really don't have
			// to synchronize at all since we're the only ones with a
			// reference to that list, but we do anyway for consistency.
			synchronized (targets)
			{
				i = targets.iterator();
				while (i.hasNext())
				{
					try
					{
						target = (Target) i.next();
						HTTPBackgroundTest httpTest =
							new HTTPBackgroundTest(target);
						synchronized (httpTest)
						{
							httpTest.startTest();
							httpTest.wait(timeout);
						}
						if (httpTest.getResult() ==
							BackgroundTest.RESULT_SUCCESS)
						{
							result = true;
						}
						else
						{
							result = false;

							if (httpTest.getResult() ==
								BackgroundTest.RESULT_NOTFINISHED)
							{
								logger.warning("Test timed out");
							}
						}

						if (result && ! target.isEnabled())
						{
							// I was tempted to log this at info but
							// if someone has their log level set to
							// warning then they'd only see the disable
							// messages and not the enable messages.
							logger.warning("Enabling: " + target);
							target.enable();
						}
						else if (! result && target.isEnabled())
						{
							logger.warning("Disabling: " + target);
							target.disable();
						}
					}
					catch (InterruptedException e)
					{
						logger.warning("Service test interrupted");
					}
				}
			}

			try
			{
				Thread.sleep(frequency);
			} catch (InterruptedException e) {}
		}
	}

	class HTTPBackgroundTest extends BackgroundTest
	{
		public HTTPBackgroundTest(Target target)
		{
			super(target);
		}

		void test()
		{
			try
			{
				String proto = "http";
				if (useSSL)
				{
					proto = "https";
				}

				URL serverURL = new URL(
					proto,
					target.getInetAddress().getHostName(),
					target.getPort(),
					path);
				logger.fine("Server URL is " + serverURL);

				logger.fine("Opening connection to server");
				HttpURLConnection conn =
					(HttpURLConnection) serverURL.openConnection();

				// Set the user agent (aka "browser") field
				conn.setRequestProperty("User-Agent", userAgent);

				// Check the returned attributes to make sure everything the
				// user required is present.
				Iterator i = requirements.entrySet().iterator();
				success = true;
				while (i.hasNext() && success)
				{
					Entry requirementEntry = (Entry) i.next();
					int requirement =
						((Integer) requirementEntry.getKey()).intValue();

					switch (requirement)
					{
					case REQUIREMENT_RESPONSE_CODE:
						int requiredCode =
							((Integer) requirementEntry.getValue()).intValue();
						logger.finer("Checking for response code: " +
							requiredCode);
						if (conn.getResponseCode() != requiredCode)
						{
							logger.warning(
								"Required response code is " + requiredCode +
								", got " + conn.getResponseCode());
								success = false;
						}
						break;
					case REQUIREMENT_CONTENT_TYPE:
						String requiredType =
							(String) requirementEntry.getValue();
						logger.finer("Checking for content type: " +
							requiredType);
						if (! conn.getContentType().equals(requiredType))
						{
							logger.warning(
								"Required content type is " + requiredType +
								", got " + conn.getContentType());
								success = false;
						}
						break;
					case REQUIREMENT_DOCUMENT_TEXT:
						String requiredText =
							(String) requirementEntry.getValue();
						logger.finer("Checking for document text: " +
							requiredText);
						BufferedReader docReader =
							new BufferedReader(
								new InputStreamReader(
									conn.getInputStream()));
						success = false;
						String docLine;
						while ((docLine = docReader.readLine()) != null)
						{
							if (docLine.indexOf(requiredText) != -1)
							{
								success = true;
								logger.finer("Required text found");
							}
						}
						if (! success)
						{
							logger.warning("Required text not found");
						}
						break;
					}
				}

				conn.disconnect();

				if (success)
				{
					logger.fine("Server met all requirements");
				}

				finished = true;
				synchronized (this)
				{
					notifyAll();
				}
			}
			catch (MalformedURLException e)
			{
				logger.warning("Error building URL: " +
					e.getMessage());

				// We don't want the server to get disabled when this happens
				finished = true;
				success = true;

				return;
			}
			catch (IOException e)
			{
				logger.warning("Error communicating with server: " +
					e.getMessage());
				return;
			}
		}
	}
}

