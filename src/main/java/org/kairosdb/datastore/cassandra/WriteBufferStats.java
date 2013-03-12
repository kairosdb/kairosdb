/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.datastore.cassandra;

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
