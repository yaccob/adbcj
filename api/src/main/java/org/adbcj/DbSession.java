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

public interface DbSession {

    /**
     * Begin's a transaction using the default transaction isolation level.
     *
     * The transaction is started immediately and all subsequent operations are executed
     * in a transaction, until {@link #commit()} or {@link #rollback()} is called. This call
     * also doesn't block: The communication to establish the transaction are executed in the background
     * before the next call on the connection object.
     */
    void beginTransaction();


    /**
     * Commits this transaction.
     * The future returned will complete if the transaction has been committed successfully,
     * or contain the error when the commit failed.
     *
     * After calling commit, the connection is again in auto-commit mode.
    * @return future which signals the success, or contains the error
    */
	DbFuture<Void> commit();

    /**
     * Commits this transaction.
     * The future returned will complete if the transaction has been rolled back successfully.
     * Rollbacks should not fail, however it it does the future will contain the error.
     *
     * Rolling back a transaction might cancel all requests which have been made for this transaction.
     *
     * After calling rollback, the connection is again in auto-commit mode.
     * @return future which signals the success, or contains the error
     */
	DbFuture<Void> rollback();
	
	/**
	 * Indicates whether or not the current session is involved in a transaction.
	 * 
	 * @return  true if the session is in a transaction, false otherwise
	 */
	boolean isInTransaction();


	DbFuture<ResultSet> executeQuery(String sql);
	
	<T> DbFuture<T> executeQuery(String sql,
                                        ResultHandler<T> eventHandler,
                                        T accumulator);

	DbFuture<Result> executeUpdate(String sql);

	DbFuture<PreparedQuery> prepareQuery(String sql);
	DbFuture<PreparedUpdate> prepareUpdate(String sql);

    /**
     * Closes this connection releases its resources.
     *
     * This will close the connection gracefully {@link CloseMode#CLOSE_GRACEFULLY}. Running operations
     * will finish before the manager is closed. However no new requests will be accepted
     *
     * @return  a future object that will complete when the connection has closed
     * @throws DbException  if there's an error closing all the database connections
     */
	DbFuture<Void> close() throws DbException;
    /**
     * Closes this connection releases its resources.
     *
     * This will close the according to the given {@link CloseMode}.
     *
     * @return  a future object that will complete when the connection has closed
     * @throws DbException  if there's an error closing all the database connections
     */
	DbFuture<Void> close(CloseMode closeMode) throws DbException;

	boolean isClosed() throws DbException;
	boolean isOpen() throws DbException;

}
