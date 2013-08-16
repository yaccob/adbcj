package org.adbcj.dbcj;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * Created with IntelliJ IDEA.
 * User: fooling
 * Date: 13-8-16
 * Time: 下午1:23
 * To change this template use File | Settings | File Templates.
 */


@Test
public class ConnectionTest {
    @Parameters({"url","user","password"})
    @Test
    public void assumeIsAdbcjProtocol(String url) throws Exception{
        if (!url.startsWith("adbcj:")){
            Assert.fail();
        }

    }
}
