/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * Performs service tests against an LDAP server
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

import java.util.Hashtable;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.logging.Logger;
import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.ldap.*;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class LDAPServiceTest implements Runnable
{
	Distributor distributor;
	Logger logger;

	Hashtable env;

	int frequency;  // How often should the test be done?
	int timeout;  // How long do we wait for the test to complete before
	              // deciding that it has failed?

	String searchDN;

	Map requiredAttributes;
	String[] requiredAttributeNames;

	int sslType;

	Thread thread;

	// SSL types for sslType parameter to constructor
	public static final int SSL_NONE = 0;
	public static final int SSL_LDAPS = 1;
	public static final int SSL_STARTTLS = 2;

	public LDAPServiceTest(Distributor distributor, Element configElement)
	{
		this.distributor = distributor;
		logger = distributor.getLogger();

		env = new Hashtable();
		env.put(Context.INITIAL_CONTEXT_FACTORY,
			"com.sun.jndi.ldap.LdapCtxFactory");

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

		if (configElement.getAttribute("ssl_type").equals("starttls"))
		{
			sslType = SSL_STARTTLS;
		}
		else if (configElement.getAttribute("ssl_type").equals("ldaps"))
		{
			sslType = SSL_LDAPS;
		}
		else
		{
			sslType = SSL_NONE;
		}
		logger.fine("SSL type:  " + sslType);

		if (! configElement.getAttribute("ssl_keystore").equals(""))
		{
			System.setProperty(
				"javax.net.ssl.trustStore",
				configElement.getAttribute("ssl_keystore"));
		}
		logger.fine("SSL keystore:  " +
			System.getProperty("javax.net.ssl.trustStore"));

		// *** All of the auth related attributes are ignored for now

		// Extract search DN and required attributes from XML document.
		// Document structure:
		// 
		// <query dn="value">
		//   <required_attribute name"attr" value="attrvalue"/>
		// </query>
		searchDN = null;
		requiredAttributes = new LinkedHashMap();
		NodeList configChildren = configElement.getChildNodes();
		for (int i=0 ; i<configChildren.getLength() ; i++)
		{
			Node configNode = configChildren.item(i);
			if (configNode.getNodeName().equals("query"))
			{
				Element queryElement = (Element) configNode;
				searchDN = queryElement.getAttribute("dn");
				NodeList queryChildren = queryElement.getChildNodes();
				for (int j=0 ; j<queryChildren.getLength() ; j++)
				{
					Node queryNode = queryChildren.item(j);
					if (queryNode.getNodeName().equals("required_attribute"))
					{
						Element reqAttrElement = (Element) queryNode;
						if (reqAttrElement.getAttribute("value").equals(""))
						{
							requiredAttributes.put(
								reqAttrElement.getAttribute("name"),
								null);
						}
						else
						{
							requiredAttributes.put(
								reqAttrElement.getAttribute("name"),
								reqAttrElement.getAttribute("value"));
						}
					}
				}
			}
		}

		if (searchDN == null)
		{
			logger.severe("A search DN is required");
			System.exit(1);  // ***
		}
		if (requiredAttributes.size() == 0)
		{
			logger.severe("At least one required attribute must be " +
				"specified");
			System.exit(1);  // ***
		}

		logger.fine("Search DN:  " + searchDN);
		logger.fine("Required attributes:  " + requiredAttributes);

		// Populate requiredAttributeNames with a list of the keys of
		// the requiredAttributes hash.  This is used later to tell the
		// server which attributes we want returned.  Since we're only
		// going to be checking the results for attributes in
		// requiredAttributes, we might as well tell the server only to
		// return those attributes.
		requiredAttributeNames = new String[requiredAttributes.keySet().size()];
		Iterator i = requiredAttributes.keySet().iterator();
		int j = 0;
		while (i.hasNext())
		{
			String attrName = (String) i.next();
			logger.fine("Adding " + attrName + " to result list");
			requiredAttributeNames[j] = attrName;
			j++;
		}

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
						//result = test(
							//target.getInetAddress().getHostName(),
							//target.getPort());
						LDAPBackgroundTest ldapTest =
							new LDAPBackgroundTest(target);
						synchronized (ldapTest)
						{
							ldapTest.startTest();
							ldapTest.wait(timeout);
						}
						if (ldapTest.getResult() ==
							BackgroundTest.RESULT_SUCCESS)
						{
							result = true;
						}
						else
						{
							result = false;
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

	class LDAPBackgroundTest extends BackgroundTest
	{
		public LDAPBackgroundTest(Target target)
		{
			super(target);
		}

		void test()
		{
			String serverURL =
				"ldap://" +
				target.getInetAddress().getHostName() + ":" +
				target.getPort() + "/";
			logger.fine("Server URL is " + serverURL);
			env.put(Context.PROVIDER_URL, serverURL);

			if (sslType == SSL_LDAPS)
			{
				logger.finer("Enabling SSL (ldaps)");
				env.put(Context.SECURITY_PROTOCOL, "ssl");
			}
			else
			{
				// In case a previous test set it
				env.remove(Context.SECURITY_PROTOCOL);
			}

			try
			{
				logger.finest("Creating context");
				LdapContext ctx = new InitialLdapContext(env, null);

				if (sslType == SSL_STARTTLS)
				{
					logger.finer("Starting TLS");
					StartTlsResponse tls =
						(StartTlsResponse) ctx.extendedOperation(
							new StartTlsRequest());
					SSLSession sess = tls.negotiate();
				}

				logger.fine("Getting attributes from server");
				Attributes returnedAttributes =
					ctx.getAttributes(searchDN, requiredAttributeNames);

				// Check the returned attributes to make sure everything the
				// user required is present.
				Iterator i = requiredAttributes.entrySet().iterator();
				success = true;
				while (i.hasNext() && success)
				{
					Entry reqAttr = (Entry) i.next();
					String reqAttrKey = (String) reqAttr.getKey();
					String reqAttrValue = (String) reqAttr.getValue();

					logger.finer("Checking for attribute: " + reqAttr);

					Attribute returnedAttr = returnedAttributes.get(reqAttrKey);
					if (returnedAttr == null)
					{
						logger.warning("Required attribute " +
							reqAttrKey + " not in returned attributes " +
							returnedAttributes);
						success = false;
					}
					else if (reqAttrValue != null)
					{
						if (! returnedAttr.contains(reqAttrValue))
						{
							logger.warning("Required attribute value " +
								reqAttrValue + " not in returned values " +
								returnedAttr);
							success = false;
						}
					}
				}
	
				logger.finest("Closing context");
				ctx.close();

				if (success)
				{
					logger.fine("Server returned all required attributes");
				}

				finished = true;
				synchronized (this)
				{
					notifyAll();
				}
			}
			catch (NamingException e)
			{
				logger.warning("Error communicating with LDAP server: " +
					e.getMessage());
				return;
			}
			catch (IOException e)
			{
				logger.warning("Error negotiating TLS with LDAP server: " +
					e.getMessage());
				return;
			}
		}
	}
}

