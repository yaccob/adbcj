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

import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;

public class MySqlClientEncoder {

	public void encode(ClientRequest request, OutputStream out) throws IOException, NoSuchAlgorithmException {
        if(request.startWriteOrCancel()){
            int length = request.getLength();

            // Write the length of the packet
            out.write(length & 0xFF);
            out.write(length >> 8 & 0xFF);
            out.write(length >> 16 & 0xFF);


            // Write the packet number
            out.write(request.getPacketNumber());

            request.writeToOutputStream(out);
        }


	}


}
