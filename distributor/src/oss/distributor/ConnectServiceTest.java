/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * Performs simple TCP connection service test
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
import java.net.Socket;
import java.util.List;
import java.util.Iterator;
import java.util.logging.Logger;
import org.w3c.dom.Element;

class ConnectServiceTest implements Runnable
{
	Distributor distributor;
	Logger logger;

	int frequency;  // How often should the test be done?
	int timeout;  // How long do we wait for the test to complete before
	              // deciding that it has failed?

	Thread thread;

	/*
	 * Because the service tests are instantiated via Class.forName(),
	 * they must have public constructors.
	 */
	public ConnectServiceTest(Distributor distributor, Element configElement)
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
		logger.config("Test frequency:  " + frequency);

		timeout = 2000;  // Default of 2s
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
		logger.config("Test timeout:  " + timeout);

		thread = new Thread(this, getClass().getName());
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
						ConnectBackgroundTest connectTest =
							new ConnectBackgroundTest(target);
						synchronized (connectTest)
						{
							connectTest.startTest();
							connectTest.wait(timeout);
						}
						if (connectTest.getResult() ==
							BackgroundTest.RESULT_SUCCESS)
						{
							result = true;
						}
						else
						{
							result = false;

							if (connectTest.getResult() ==
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

	class ConnectBackgroundTest extends BackgroundTest
	{
		protected ConnectBackgroundTest(Target target)
		{
			super(target);
		}

		public void test()
		{
			try
			{
				Socket sock = new Socket(
					target.getInetAddress(),
					target.getPort());

				sock.close();

				success = true;
				finished = true;
				synchronized (this)
				{
					notify();
				}
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

