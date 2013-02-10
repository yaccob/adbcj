/*
 *   Copyright (c) 2007 Mike Heath.  All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package org.adbcj;

/**
 * A connection to a specific database.  <code>Connection</code> objects are obtained using
 * {@link ConnectionManager#connect()}.
 *
 * Remember to close a connection after using it with {@link #close()}
 *
 * @author Mike Heath
 * @see DbSession
 * @see ConnectionManager
 */
public interface Connection extends DbSession {

	/**
	 * Returns the instance of the connection manager that created this connection.
	 * 
	 * @return  The connection manager instance that created this connection.
	 */
	ConnectionManager getConnectionManager();

	
}
