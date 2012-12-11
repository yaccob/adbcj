package org.adbcj.h2.server.decoding;

import org.adbcj.h2.decoding.Constants;
import org.adbcj.h2.decoding.IoUtils;
import org.adbcj.h2.decoding.ResultOrWait;
import org.adbcj.h2.server.responses.AnnounceUsedVersion;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Engine;
import org.h2.engine.Session;
import org.jboss.netty.channel.Channel;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class ServerHandshakeStart implements DecoderState {
    @Override
    public ResultAndState decode(DataInputStream stream, Channel channel) throws IOException {
        final ResultOrWait<Integer> minVersion = IoUtils.tryReadNextInt(stream, ResultOrWait.Start);
        final ResultOrWait<Integer> maxVersion = IoUtils.tryReadNextInt(stream, minVersion);
        final ResultOrWait<String> database = IoUtils.tryReadNextString(stream, maxVersion);
        final ResultOrWait<String> originalUrl = IoUtils.tryReadNextString(stream, database);
        final ResultOrWait<String> userName = IoUtils.tryReadNextString(stream, originalUrl);
        final ResultOrWait<byte[]> pwdHash = IoUtils.tryReadNextBytes(stream, userName);
        final ResultOrWait<byte[]> filePwdHash = IoUtils.tryReadNextBytes(stream, pwdHash);
        final ResultOrWait<Integer> keysCount = IoUtils.tryReadNextInt(stream, filePwdHash);
        final ResultOrWait<Map<String, String>> properties = readKeys(stream, keysCount);
        if (properties.couldReadResult) {
            if (Constants.TCP_PROTOCOL_VERSION_12 >= minVersion.result &&
                    Constants.TCP_PROTOCOL_VERSION_12 <= maxVersion.result) {
                ConnectionInfo ci = new ConnectionInfo(database.result);
                ci.setOriginalURL(originalUrl.result);
                ci.setUserName(userName.result);
                ci.setUserPasswordHash(pwdHash.result);
                ci.setFilePasswordHash(filePwdHash.result);

                for (Map.Entry<String, String> props : properties.result.entrySet()) {
                    ci.setProperty(props.getKey(), props.getValue());
                }
                final Session session = Engine.getInstance().createSession(ci);
                channel.write(new AnnounceUsedVersion(Constants.TCP_PROTOCOL_VERSION_12));
                return ResultAndState.newState(new AcceptCommands(session));
            } else {
                throw new IllegalStateException("Cannot deal with this client." +
                        " Need version at least" + Constants.TCP_PROTOCOL_VERSION_12);
            }

        } else {
            return ResultAndState.waitForMoreInput(this);
        }


    }

    private ResultOrWait<Map<String, String>> readKeys(DataInputStream stream, ResultOrWait<Integer> keysCount) throws IOException {
        if (keysCount.couldReadResult) {
            int keysToRead = keysCount.result;
            Map<String, String> keys = new HashMap<String, String>(keysToRead);
            for (int i = 0; i < keysToRead; i++) {
                final ResultOrWait<String> key = IoUtils.tryReadNextString(stream, keysCount);
                final ResultOrWait<String> value = IoUtils.tryReadNextString(stream, key);
                if (!value.couldReadResult) {
                    return ResultOrWait.WaitLonger;
                } else {
                    keys.put(key.result, value.result);
                }

            }
            return ResultOrWait.result(keys);
        } else {
            return ResultOrWait.WaitLonger;
        }
    }
}
