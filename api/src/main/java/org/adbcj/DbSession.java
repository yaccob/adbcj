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
     */
    void beginTransaction();

    /**
     * Begin's a transaction with the specific transaction isolation level.
     *
     * @param isolationLevel  the transaction isolation level to use in the new transaction
     */
    //void beginTransaction(TransactionIsolationLevel isolationLevel);

    // Canceling a commit will cause the transaction to rollback
	DbSessionFuture<Void> commit();

	// A rollback cannot be canceled
	// Rolling back a transaction may cancel pending requests
	DbSessionFuture<Void> rollback();
	
	/**
	 * Indicates whether or not the current session is involved in a transaction.
	 * 
	 * @return  true if the session is in a transaction, false otherwise
	 */
	boolean isInTransaction();

    /**
     * Returns the isolation level of the current transaction, returns null if the DbSession is not
     * currently in a transaction.
     *
     * @return  the current transaction isolation level
     */
    //TransactionIsolationLevel getTransactionIsolationLevel();

	DbSessionFuture<ResultSet> executeQuery(String sql);
	
	<T> DbSessionFuture<T> executeQuery(String sql,
                                        ResultHandler<T> eventHandler,
                                        T accumulator);

	DbSessionFuture<Result> executeUpdate(String sql);

	DbSessionFuture<PreparedQuery> prepareQuery(String sql);
	DbSessionFuture<PreparedUpdate> prepareUpdate(String sql);

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
