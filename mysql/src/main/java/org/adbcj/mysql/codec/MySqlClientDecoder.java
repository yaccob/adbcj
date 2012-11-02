/*
	This file is part of ADBCJ.

	ADBCJ is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	ADBCJ is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with ADBCJ.  If not, see <http://www.gnu.org/licenses/>.

	Copyright 2008  Mike Heath
 */
package org.adbcj.mysql.codec;

import org.adbcj.mysql.codec.decoding.DecoderState;
import org.adbcj.mysql.codec.decoding.ResultAndState;
import org.adbcj.mysql.codec.packets.FailedToParseInput;
import org.adbcj.mysql.codec.packets.ResponseExpected;
import org.adbcj.mysql.codec.packets.ServerPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Client stateful decoder.  Being stateful, each client connection must have its own decoder instance to function
 * properly.
 *
 * @author Mike Heath <mheath@apache.org>
 */
public class MySqlClientDecoder {
    private static final Logger logger = LoggerFactory.getLogger(MySqlClientDecoder.class);


    private DecoderState state = DecoderState.CONNECTING;

    /**
     * Decodes a message from a MySql server.
     *
     * @param input the {@code InputStream} from which to decode the message
     * @param block true if the decoder can block, false otherwise
     * @return the decode message, null if the {@code block} is {@code} false and there is not enough data available
     *         to decode the message without blocking
     * @throws IOException thrown if an error occurs reading data from the inputstream
     */
    public ServerPacket decode(AbstractMySqlConnection connection, InputStream input, boolean block) throws IOException {
        // If mark is not support and we can't block, throw an exception
        if (!input.markSupported() && !block) {
            throw new IllegalArgumentException("Non-blocking decoding requires an InputStream that supports marking");
        }
        // TODO This should be the max packet size - make this configurable
        input.mark(Integer.MAX_VALUE);
        ServerPacket message = null;
        try {
            ServerPacket msg = doDecode(connection, input, block);
            if(state==DecoderState.RESPONSE){
                message =  new ResponseExpected(msg);
            } else{
                message = msg;
            }
        } finally {
            if (message == null) {
                input.reset();
            }
        }
        return message;
    }

    protected ServerPacket doDecode(AbstractMySqlConnection connection, InputStream input, boolean block) throws IOException {
        // If we can't block, make sure there's enough data available to read
        if (!block) {
            if (input.available() < 3) {
                return null;
            }
        }
        // Read the packet length
        final int length = IoUtils.readUnsignedMediumInt(input);

        // If we can't block, make sure the stream has enough data
        if (!block) {
            // Make sure we have enough data for the packet length and the packet number
            if (input.available() < length + 1) {
                return null;
            }
        }
        final int packetNumber = IoUtils.safeRead(input);
        BoundedInputStream in = new BoundedInputStream(input, length);
        logger.trace("Decoding in state {}", state);
        ResultAndState stateAndResult = state.parse(length, packetNumber, in, connection);
        state = stateAndResult.getNewState();
        if (in.getRemaining() > 0) {
            final String message = "Didn't read all input. Maybe this input belongs to a failed request. " +
                    "Remaining bytes: " + in.getRemaining();
            return new FailedToParseInput(length, packetNumber, new IllegalStateException(message));
        }
        return stateAndResult.getResult();

    }




    /**
     * Sets the state, used for testing.
     *
     * @param state
     */
    void setState(DecoderState state) {
        this.state = state;
    }
}
