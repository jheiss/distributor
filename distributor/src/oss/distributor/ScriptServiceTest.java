/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * Calls a user supplied program to perform testing of back end servers
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
import java.util.Iterator;
import java.util.logging.Logger;
import java.io.IOException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ScriptServiceTest implements Runnable
{
	Distributor distributor;
	Logger logger;

	int frequency;  // How often should the test be done?
	int timeout;  // How long do we wait for the test to complete before
	              // deciding that it has failed?

	String script;

	Thread thread;

	public ScriptServiceTest(Distributor distributor, Element configElement)
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

		script = configElement.getAttribute("script");
		logger.fine("Script:  " + script);
		if (script == null)
		{
			logger.severe("A script is required");
			System.exit(1);  // ***
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

						ScriptBackgroundTest scriptTest =
							new ScriptBackgroundTest(target);

						synchronized (scriptTest)
						{
							scriptTest.startTest();
							scriptTest.wait(timeout);
						}

						if (scriptTest.getResult() ==
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

	class ScriptBackgroundTest extends BackgroundTest
	{
		public ScriptBackgroundTest(Target target)
		{
			super(target);
		}

		void test()
		{
			String server = target.getInetAddress().getHostName();
			int port = target.getPort();

			try
			{
				String[] cmdarray = new String[3];
				cmdarray[0] = script;
				cmdarray[1] = server;
				cmdarray[2] = Integer.toString(port);

				logger.fine("Executing script");
				Process proc = Runtime.getRuntime().exec(cmdarray);

				boolean procDone = false;
				while (!procDone)
				{
					try
					{
						proc.waitFor();
						procDone = true;
					}
					catch (InterruptedException e) {}
				}

				if (proc.exitValue() == 0)
				{
					logger.fine("Script returned zero, server fine");
					success = true;
				}
				else
				{
					logger.warning(
						"Script returned non-zero, server has failed");
					success = false;
				}

				finished = true;
				synchronized (this)
				{
					notifyAll();
				}
			}
			catch (IOException e)
			{
				logger.warning("Error when executing script: " +
					e.getMessage());

				// The most likely causes of an IOException are that
				// the admin supplied a non-existent script or we don't
				// have permission to execute it.
				// We don't want the server to get disabled when this happens
				finished = true;
				success = true;

				return;
			}
		}
	}
}

