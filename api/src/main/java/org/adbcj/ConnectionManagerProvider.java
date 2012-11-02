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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class ConnectionManagerProvider {

	public static final String ADBCJ_PROTOCOL = "adbcj";

	private ConnectionManagerProvider () {}
	
	public static ConnectionManager createConnectionManager(String url, String username, String password) throws DbException {
		return createConnectionManager(url, username, password, Collections.<String,String>emptyMap());
	}

	public static ConnectionManager createConnectionManager(String url,
                                                            String username,
                                                            String password,
                                                            final Map<String,String> properties) throws DbException {
		if (url == null) {
			throw new IllegalArgumentException("Connection url can not be null");
		}

        Map<String,String> propertiesWithStandard = addStandardSettings(properties);
		
		try {
			URI uri = new URI(url);
			String adbcjProtocol = uri.getScheme();
			if (!ADBCJ_PROTOCOL.equals(adbcjProtocol)) {
				throw new DbException("Invalid connection URL: " + url);
			}
			URI driverUri = new URI(uri.getSchemeSpecificPart());
			String protocol = driverUri.getScheme();

			ServiceLoader<ConnectionManagerFactory> serviceLoader = ServiceLoader.load(ConnectionManagerFactory.class);
			for (ConnectionManagerFactory factory : serviceLoader) {
				if (factory.canHandle(protocol)) {
					return factory.createConnectionManager(url, username, password, propertiesWithStandard);
				}
			}
			throw new DbException("Could not find ConnectionManagerFactory for protocol '" + protocol + "'");
		} catch (URISyntaxException e) {
			throw new DbException("Invalid connection URL: " + url);
		}
	}

    private static Map<String, String> addStandardSettings(Map<String, String> userProperties) {
        HashMap<String,String> newMap = new HashMap<String, String>();
        newMap.put(StandardProperties.MAX_QUEUE_LENGTH,"64");
        for (Map.Entry<String, String> entry : userProperties.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue());
        }
        return newMap;
    }

}
