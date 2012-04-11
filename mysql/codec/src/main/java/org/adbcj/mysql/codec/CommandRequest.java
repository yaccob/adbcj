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

import java.io.UnsupportedEncodingException;


public class CommandRequest extends ClientRequest {

    private final Command command;

    public CommandRequest(Command command) {
        this.command = command;
    }

    public Command getCommand() {
        return command;
    }

    @Override
    public int getLength(String charset) throws UnsupportedEncodingException {
        return 1;
    }

}

class StringCommandRequest extends CommandRequest {
    private final String payload;

    public StringCommandRequest(Command command, String payload) {
        super(command);
        this.payload = payload;
    }


    @Override
    public int getLength(String charset) throws UnsupportedEncodingException {
        return 1 + payload.getBytes(charset).length;
    }

    public String getPayload() {
        return payload;
    }


}
