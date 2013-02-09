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
package org.adbcj.mysql.codec.packets;

import org.adbcj.mysql.codec.ClientRequest;
import org.adbcj.support.CancellationToken;

import java.io.IOException;
import java.io.OutputStream;


public class CommandRequest extends ClientRequest {

    private final Command command;


    public CommandRequest(Command command) {
        this(command,CancellationToken.NO_CANCELLATION);
    }

    public CommandRequest(Command command, CancellationToken cancelSupport) {
        super(cancelSupport);
        this.command = command;
    }

    public Command getCommand() {
        return command;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    protected boolean hasPayload() {
        return false;
    }

    @Override
    public final void writeToOutputStream(OutputStream out) throws IOException {
        out.write(command.getCommandCode());
        if(hasPayload()){
            writePayLoad(out);
        }
    }

    protected void writePayLoad(OutputStream out) throws IOException{

    }

    @Override
    public String toString() {
        return "CommandRequest{" +
                "command=" + command +
                '}';
    }
}

