package org.adbcj.mysql.codec;

import org.adbcj.*;
import org.adbcj.mysql.codec.packets.*;
import org.adbcj.support.AbstractDbSession.Request;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.ExpectResultRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Asynchronous protocol handler suitable for use with frameworks like MINA or Netty.
 *
 * @author Mike Heath <mheath@apache.org>
 */
public class ProtocolHandler {
    private final Logger logger = LoggerFactory.getLogger(ProtocolHandler.class);

    public void connectionClosed(AbstractMySqlConnection connection) throws Exception {
        logger.trace("IoSession closed");
        connection.doClose();
    }

    /**
     * @return any exception that couldn't be handled, null if the exception was successfully handled
     * @throws Exception
     */
    public Throwable handleException(AbstractMySqlConnection connection, Throwable cause) throws Exception {
        logger.debug("Caught exception: ", cause);

        DbException dbException = DbException.wrap( cause);
        if (connection != null) {
            DefaultDbFuture<Connection> connectFuture = connection.getConnectFuture();
            if (!connectFuture.isDone()) {
                connectFuture.setException(dbException);
                return null;
            }
            Request<?> activeRequest = connection.getActiveRequest();
            if (activeRequest != null) {
                if(activeRequest instanceof ExpectResultRequest){
                    ExpectResultRequest<ResultSet> resultHandlingRequest = (ExpectResultRequest<ResultSet>) activeRequest;
                    resultHandlingRequest.getEventHandler().exception(cause,resultHandlingRequest.getAccumulator() );
                }
                if (!activeRequest.isDone()) {
                    try {
                        activeRequest.error(dbException);

                        return null;
                    } catch (Throwable e) {
                        return e;
                    }
                }
            }
        }
        return dbException;
    }

    public void messageReceived(AbstractMySqlConnection connection, Object message) throws Exception {
        logger.trace("Received message: {}", message);
        if (message instanceof ServerGreeting) {
            handleServerGreeting(connection, (ServerGreeting) message);
        } else if (message instanceof OkResponse.RegularOK) {
            handleOkResponse(connection, ((OkResponse.RegularOK) message));
        } else if (message instanceof OkResponse.PreparedStatementOK) {
            // no action right now
        } else if (message instanceof PreparedStatementToBuild) {
            // no action right now
        } else if (message instanceof StatementPreparedEOF) {
            handlePreparedStatement(connection, (StatementPreparedEOF) message);
        } else if (message instanceof ErrorResponse) {
            handleErrorResponse(connection, (ErrorResponse) message);
        } else if (message instanceof ResultSetResponse) {
            handleResultSetResponse(connection, (ResultSetResponse) message);
        } else if (message instanceof ResultSetFieldResponse) {
            handleResultSetFieldResponse(connection, (ResultSetFieldResponse) message);
        } else if (message instanceof ResultSetRowResponse) {
            handleResultSetRowResponse(connection, (ResultSetRowResponse) message);
        } else if (message instanceof EofResponse) {
            handleEofResponse(connection, (EofResponse) message);
        } else {
            throw new IllegalStateException("Unable to handle message of type: " + message.getClass().getName());
        }
    }

    private void handlePreparedStatement(AbstractMySqlConnection connection, StatementPreparedEOF preparationInfo) {
        AbstractMySqlConnection.PreparedStatementRequest activeRequest
                = (AbstractMySqlConnection.PreparedStatementRequest) connection.<PreparedUpdate>getActiveRequest();
        activeRequest.complete(new MySqlPreparedStatement(connection, preparationInfo));
    }

    private void handleServerGreeting(AbstractMySqlConnection connection, ServerGreeting serverGreeting) {
        // TODO save the parts of the greeting that we might need (like the protocol version, etc.)
        // Send Login request
        LoginRequest request = new LoginRequest(connection.getCredentials(),
                connection.getClientCapabilities(),
                connection.getExtendedClientCapabilities(),
                connection.getCharacterSet(), serverGreeting.getSalt());
        connection.write(request);
    }

    private void handleOkResponse(AbstractMySqlConnection connection, OkResponse.RegularOK response) {
        logger.trace("Response '{}' on connection {}", response, connection);

        List<String> warnings = new ArrayList<String>(response.getWarningCount());
        if (response.getWarningCount() > 0) {
            for (int i = 0; i < response.getWarningCount(); i++) {
                warnings.add(response.getMessage());
            }
        }

        logger.warn("Warnings: {}", warnings);

        Request<Result> activeRequest = connection.getActiveRequest();
        if (activeRequest == null) {
            // TODO Do we need to pass the warnings on to the connection?
            DefaultDbFuture<Connection> connectFuture = connection.getConnectFuture();
            if (!connectFuture.isDone()) {
                connectFuture.setResult(connection);

                return;
            } else {
                throw new IllegalStateException("Received an OkResponse with no activeRequest " + response);
            }
        }
        Result result = new MysqlResult(response.getAffectedRows(), warnings,response.getInsertId());
        activeRequest.complete(result);
    }

    private void handleErrorResponse(AbstractMySqlConnection connection, ErrorResponse message) {
        throw new MysqlException(message.getSqlState() + " " + message.getMessage());
    }

    private void handleResultSetResponse(AbstractMySqlConnection connection, ResultSetResponse message) {
        ExpectResultRequest<ResultSet> activeRequest = (ExpectResultRequest<ResultSet>) connection.<ResultSet>getActiveRequest();

        if (activeRequest == null) {
            throw new IllegalStateException("No active request for response: " + message);
        }

        logger.debug("Start field definitions");
        activeRequest.getEventHandler().startFields(activeRequest.getAccumulator());
    }

    private void handleResultSetFieldResponse(AbstractMySqlConnection connection, ResultSetFieldResponse message) {
        ExpectResultRequest<ResultSet> activeRequest = (ExpectResultRequest<ResultSet>) connection.<ResultSet>getActiveRequest();

        ResultSetFieldResponse fieldResponse = (ResultSetFieldResponse) message;
        activeRequest.getEventHandler().field(fieldResponse.getField(), activeRequest.getAccumulator());
    }

    private void handleResultSetRowResponse(AbstractMySqlConnection connection, ResultSetRowResponse message) {
        ExpectResultRequest<ResultSet> activeRequest = (ExpectResultRequest<ResultSet>) connection.<ResultSet>getActiveRequest();

        ResultSetRowResponse rowResponse = (ResultSetRowResponse) message;

        activeRequest.getEventHandler().startRow(activeRequest.getAccumulator());
        for (Value value : rowResponse.getValues()) {
            activeRequest.getEventHandler().value(value, activeRequest.getAccumulator());
        }
        activeRequest.getEventHandler().endRow(activeRequest.getAccumulator());
    }

	private void handleEofResponse(AbstractMySqlConnection connection, EofResponse message) {
		logger.trace("Fetching active request in handleEofResponse()");
        ExpectResultRequest<ResultSet> activeRequest = (ExpectResultRequest<ResultSet>)connection.<ResultSet>getActiveRequest();

		if (activeRequest == null) {
			throw new IllegalStateException("No active request for response: " + message);
		}

		EofResponse eof = (EofResponse)message;
		switch (eof.getType()) {
		case FIELD:
			activeRequest.getEventHandler().endFields(activeRequest.getAccumulator());
			activeRequest.getEventHandler().startResults(activeRequest.getAccumulator());
			break;
		case ROW:
			activeRequest.getEventHandler().endResults(activeRequest.getAccumulator());
			activeRequest.complete(activeRequest.getAccumulator());
			break;
		default:
			throw new MysqlException("Unkown eof response type");
		}
	}

}
