/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * This class allows you to start a test which runs in the background.
 * You can let it run as long as you like, checking occasionally to see
 * if it has completed and eventually giving up on it if desired.
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

import java.net.InetAddress;

public abstract class BackgroundTest implements Runnable
{
	boolean finished = false;  // Has the test completed?
	boolean success = false;  // Did the test succeed?
	Target target;
	Thread thread;

	public BackgroundTest(Target target)
	{
		this.target = target;

		thread = new Thread(this, getClass().getName());
	}

	public void startTest()
	{
		thread.start();
	}

	public void run()
	{
		test();
	}

	/*
	 * The test method should set finished to true when complete and
	 * success to true if the test succeeded.
	 *
	 * It should also call this.notify() when finished to allow
	 * the caller to wait() on this object.
	 */
	public abstract void test();

	public static final int RESULT_NOTFINISHED = 1;
	public static final int RESULT_SUCCESS = 2;
	public static final int RESULT_FAILURE = 3;
	public int getResult()
	{
		if (!finished)
		{
			return RESULT_NOTFINISHED;
		}
		else if (success)
		{
			return RESULT_SUCCESS;
		}
		else
		{
			return RESULT_FAILURE;
		}
	}
}

