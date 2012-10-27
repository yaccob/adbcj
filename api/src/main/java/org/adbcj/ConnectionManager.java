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
 * Manages a set of {@link Connection} instances.  A database connection is established by invoking {@link #connect()}.
 * All the connections managed by this connection manager can be closed using {@link #close(boolean)}.
 * 
 * @author Mike Heath
 */
public interface ConnectionManager extends DbSessionProvider {
	
	/**
	 * Establishes a new database connection.
	 * 
	 * @return  a future object that will complete when the database connection has been established or failed.
	 */
	DbFuture<Connection> connect();
	
	/**
	 * Closes all the database connections managed by this {@code ConnectionManager} and releases any resources
	 * used for managing asynchronous database connections.
     *
     * This will close the connections gracefully {@link CloseMode#CLOSE_GRACEFULLY}. Running operations
     * will finish before the manager is closed. The behavior is the same as calling close(CloseMode.LOSE_GRACEFULLY)
	 *
	 * @return  a future object that will complete when all database connections managed by this
	 *          {@code ConnectionManager} have closed.
	 * @throws DbException  if there's an error closing all the database connections
	 */
	DbFuture<Void> close() throws DbException;

    /**
     * Closes all the database connections managed by this {@code ConnectionManager} and releases any resources
     * used for managing asynchronous database connections.
     *
     * Closes according to the given {@link CloseMode}.
     *
     * @return  a future object that will complete when all database connections managed by this
     *          {@code ConnectionManager} have closed.
     * @throws DbException  if there's an error closing all the database connections
     */
	DbFuture<Void> close(CloseMode mode) throws DbException;

	/**
	 * Indicates if this {@code ConnectionManager} is closed.
	 * 
	 * @return  true if this {@code ConnectionManager} is closed, false otherwise.
	 */
	boolean isClosed();

}
