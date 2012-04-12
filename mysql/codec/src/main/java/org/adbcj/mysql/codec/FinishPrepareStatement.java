package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.OkResponse;
import org.adbcj.mysql.codec.packets.StatementPreparedEOF;

import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
abstract class FinishPrepareStatement extends DecoderState {

    protected final OkResponse.PreparedStatementOK statement;

    FinishPrepareStatement(OkResponse.PreparedStatementOK statement) {
        this.statement = statement;
    }

    protected void readAllAndIgnore(BoundedInputStream in) throws IOException {
        in.read(new byte[in.getRemaining()]);
    }

    public static DecoderState create(OkResponse.PreparedStatementOK statement) {
        if(statement.getParams()>0){
            return new ReadParameters(statement.getParams(),statement);
        }else if(statement.getColumns()>0){
            return new ReadColumns(statement.getColumns(),statement);
        } else{
            throw new IllegalStateException("Should be finished allready!");
        }
    }

    private static class ReadParameters extends FinishPrepareStatement {
        private final int parametersToParse;

        public ReadParameters(int parametersToParse, OkResponse.PreparedStatementOK statement) {
            super(statement);

            this.parametersToParse = parametersToParse;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            readAllAndIgnore(in);
            int restOfParams = parametersToParse - 1;
            if (restOfParams > 0) {
                return result(new ReadParameters(restOfParams, statement), statement);
            } else {
                return result(new EofAndColumns(statement), statement);
            }
        }
    }

    private static class EofAndColumns extends FinishPrepareStatement {

        public EofAndColumns(OkResponse.PreparedStatementOK statement) {
            super(statement);
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            if (in.read() == RESPONSE_EOF) {
                EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);
                if (statement.getColumns() == 0) {
                    return result(RESPONSE, new StatementPreparedEOF(packetNumber, packetNumber, statement));
                } else {
                    return result(new ReadColumns(statement.getColumns(), statement), statement);
                }
            } else {
                throw new IllegalStateException("Did not expect a EOF from the server");
            }
        }
    }

    private static class ReadColumns extends FinishPrepareStatement {

        private final int restOfColumns;

        public ReadColumns(int restOfColumns, OkResponse.PreparedStatementOK statement) {
            super(statement);
            this.restOfColumns = restOfColumns;
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            readAllAndIgnore(in);
            int restOfParams = restOfColumns - 1;
            if (restOfParams > 0) {
                return result(new ReadColumns(restOfParams, statement), statement);
            } else {
                return result(new EofStatement(statement), statement);
            }
        }
    }

    private static class EofStatement extends FinishPrepareStatement {

        public EofStatement(OkResponse.PreparedStatementOK statement) {
            super(statement);
        }

        @Override
        public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
            if (in.read() == RESPONSE_EOF) {
                EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);
                return result(RESPONSE, new StatementPreparedEOF(packetNumber, packetNumber, statement));
            } else {
                throw new IllegalStateException("Did not expect a EOF from the server");
            }
        }
    }
}
