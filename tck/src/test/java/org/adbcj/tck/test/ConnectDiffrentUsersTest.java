package org.adbcj.tck.test;

import org.adbcj.Connection;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class ConnectDiffrentUsersTest extends AbstractWithConnectionManagerTest{

    @Parameters({"url", "user", "password"})
    @Test
    public void connectWithOtherUser() throws Exception {

        Connection normalUser = connectionManager.connect().get();
        Connection connectionOtherUser = connectionManager.connect("adbcj-other-user","adbcj-other-user").get();

        String userNormal = normalUser.executeQuery("SELECT current_user()").get().get(0).get(0).getString();
        String otherUser = connectionOtherUser.executeQuery("SELECT current_user()").get().get(0).get(0).getString();

        Assert.assertEquals(userNormal,"adbcjtck@localhost");
        Assert.assertEquals(otherUser,"adbcj-other-user@localhost");

        normalUser.close();
        connectionOtherUser.close();
    }
}
