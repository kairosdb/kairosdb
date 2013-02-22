// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>
package net.opentsdb.datastore.cassandra;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 1/25/13
 Time: 8:35 AM
 To change this template use File | Settings | File Templates.
 */
public interface WriteBufferStats
{
	/**
	 This is called right before a write to cassandra is performed.
	 If this data is being written into cassandra it must be done on a separate
	 thread from the calling thread, otherwise a deadlock could occur.
	 @param pendingWrites
	 */
	public void saveWriteSize(int pendingWrites);
}
