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

import org.adbcj.mysql.codec.BoundedInputStream;
import org.adbcj.mysql.codec.IoUtils;
import org.adbcj.mysql.codec.ServerStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class OkResponse extends ServerPacket {
    private final byte[] restToParse;

    public OkResponse(int packetLength, int packetNumber,byte[] restToParse) {
        super(packetLength, packetNumber);
        this.restToParse = restToParse;
    }
    public OkResponse(int packetLength, int packetNumber) {
        super(packetLength, packetNumber);
        restToParse = null;
    }

    public RegularOK interpretAsRegularOk() throws IOException {
        BoundedInputStream restToParse = new BoundedInputStream(new ByteArrayInputStream(this.restToParse),this.restToParse.length);
        long affectedRows = IoUtils.readBinaryLengthEncoding(restToParse);
        long insertId = IoUtils.readBinaryLengthEncoding(restToParse);
        Set<ServerStatus> serverStatus = IoUtils.readEnumSetShort(restToParse, ServerStatus.class);
        int warningCount = IoUtils.readUnsignedShort(restToParse);
        String message = IoUtils.readFixedLengthString(restToParse, restToParse.getRemaining(), "UTF8");
        return new RegularOK(getPacketLength(), getPacketNumber(), affectedRows, insertId, serverStatus, warningCount, message);
    }

    public PreparedStatementOK interpretAsPreparedStatement() throws IOException {
        BoundedInputStream restToParse = new BoundedInputStream(new ByteArrayInputStream(this.restToParse),this.restToParse.length);
        int handlerId= IoUtils.readInt(restToParse);
        int columns= IoUtils.readShort(restToParse);
        int params= IoUtils.readShort(restToParse);
        int filler = restToParse.read();
        int warnings = IoUtils.readShort(restToParse);
        return new PreparedStatementOK(getPacketLength(), getPacketNumber(),handlerId,columns,params,filler,warnings);
    }
    public static class PreparedStatementOK extends OkResponse{

        private final int handlerId;
        private final int columns;
        private final int params;
        private final int filler;
        private final int warnings;

        public PreparedStatementOK(int packetLength, int packetNumber, int handlerId, int columns, int params, int filler, int warnings) {
            super(packetLength, packetNumber);
            this.handlerId = handlerId;
            this.columns = columns;
            this.params = params;
            this.filler = filler;
            this.warnings = warnings;
        }

        @Override
        public RegularOK interpretAsRegularOk() throws IOException {
            throw new IllegalStateException("This is a PreparedStatementOK and cannot be viewed otherwise");
        }

        @Override
        public PreparedStatementOK interpretAsPreparedStatement() throws IOException {
            return this;
        }

        public int getHandlerId() {
            return handlerId;
        }

        public int getColumns() {
            return columns;
        }

        public int getParams() {
            return params;
        }

        public int getFiller() {
            return filler;
        }

        public int getWarnings() {
            return warnings;
        }
    }

    public static class RegularOK extends OkResponse{

        private final long affectedRows;
        private final long insertId;
        private final Set<ServerStatus> serverStatus;
        private final int warningCount;
        private final String message;

        public RegularOK(int length, int packetNumber, long affectedRows, long insertId, Set<ServerStatus> serverStatus, int warningCount, String message) {
            super(length, packetNumber);
            this.affectedRows = affectedRows;
            this.insertId = insertId;
            this.serverStatus = Collections.unmodifiableSet(serverStatus);
            this.warningCount = warningCount;
            this.message = message;
        }

        @Override
        public RegularOK interpretAsRegularOk() throws IOException {
            return this;
        }

        @Override
        public PreparedStatementOK interpretAsPreparedStatement() throws IOException {
            throw new IllegalStateException("This is a RegularOK and cannot be viewed otherwise");
        }

        public long getAffectedRows() {
            return affectedRows;
        }

        public long getInsertId() {
            return insertId;
        }

        public Set<ServerStatus> getServerStatus() {
            return serverStatus;
        }

        public int getWarningCount() {
            return warningCount;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return String.format("OK response (affected rows: %d, insert id: %d, warning count: %d, message: '%s', server status: %s",
                    affectedRows,
                    insertId,
                    warningCount,
                    message,
                    serverStatus.toString());
        }
    }

}
