/*
 *****************************************************************************
 * $Id$
 *****************************************************************************
 * This class represents a connection from a client through the load
 * balancer to a server.
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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

public class Connection
{
	SocketChannel client;
	SocketChannel server;
	// DataMover passes the selection keys for the two channels to us
	// once it has registered the channels with its Selector.  That way
	// we can cancel the keys if we are asked to terminate the
	// connection.  Otherwise the keys get activated and DataMover has
	// to deal with an IOException in order to realize that the
	// connection has been closed, which is messy.
	SelectionKey clientSelectionKey;
	SelectionKey serverSelectionKey;
	boolean terminated;

	public Connection(SocketChannel client, SocketChannel server)
	{
		this.client = client;
		this.server = server;
		this.clientSelectionKey = null;
		this.serverSelectionKey = null;

		terminated = false;
	}

	public SocketChannel getClient()
	{
		return client;
	}

	public SocketChannel getServer()
	{
		return server;
	}

	public void setClientSelectionKey(SelectionKey key)
	{
		clientSelectionKey = key;
	}

	public void setServerSelectionKey(SelectionKey key)
	{
		serverSelectionKey = key;
	}

	public void terminate()
	{
		try
		{
			client.close();
			server.close();
		}
		catch (IOException e) {}

		if (clientSelectionKey != null)
		{
			clientSelectionKey.cancel();
		}
		if (serverSelectionKey != null)
		{
			serverSelectionKey.cancel();
		}

		terminated = true;
	}

	public boolean isTerminated()
	{
		// Someone could close both of our channels without going
		// through our terminate method, so we check for that case
		// and update our status flag before returning it.
		if (!client.isOpen() && !server.isOpen())
		{
			terminated = true;
		}

		return terminated;
	}

	public String toString()
	{
		return("Connection from " + client + " to " + server);
	}
}

