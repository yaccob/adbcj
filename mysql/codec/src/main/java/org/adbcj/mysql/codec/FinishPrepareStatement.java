package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.packets.EofResponse;
import org.adbcj.mysql.codec.packets.OkResponse;

import java.io.IOException;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
class FinishPrepareStatement extends DecoderState {
    enum State {
        PARSING_PARAMENTERS,
        PARSING_COLUMS,
        PARSING_EOF,
    }

    private final State state;
    private final OkResponse.PreparedStatementOK statement;

    FinishPrepareStatement(State state, OkResponse.PreparedStatementOK statement) {
        this.state = state;
        this.statement = statement;
    }
    FinishPrepareStatement(OkResponse.PreparedStatementOK statement) {
        if(statement.getColumns()>0){
            state = State.PARSING_PARAMENTERS;
        }  else if(statement.getParams()>0){
            state = State.PARSING_COLUMS;
        }  else {
            state = State.PARSING_EOF;
        }
        this.statement = statement;
    }

    @Override
    public ResultAndState parse(int length, int packetNumber, BoundedInputStream in, AbstractMySqlConnection connection) throws IOException {
        switch (state) {
            case PARSING_PARAMENTERS:
                readAllAndIgnore(in);
                if (statement.getColumns() > 0) {
                    return continueWith(State.PARSING_PARAMENTERS);
                } else {
                    return continueWith(State.PARSING_EOF);
                }
            case PARSING_COLUMS:
                readAllAndIgnore(in);
                return continueWith(State.PARSING_EOF);
            case PARSING_EOF:
                if (in.read() == RESPONSE_EOF) {
                    EofResponse eof = decodeEofResponse(in, length, packetNumber, EofResponse.Type.STATEMENT);
                    return result(RESPONSE, eof);
                } else {
                    throw new IllegalStateException("Did not expect a EOF from the server");
                }
            default:
                throw new IllegalStateException("Should not reach this branch");
        }

    }

    private ResultAndState continueWith(State state) {
        return result(new FinishPrepareStatement(state, statement), statement);
    }

    private void readAllAndIgnore(BoundedInputStream in) throws IOException {
        in.read(new byte[in.getRemaining()]);
    }
}
