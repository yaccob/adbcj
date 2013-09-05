package org.adbcj.tck.test;
import junit.framework.Assert;
import org.adbcj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author foooling@gmail.com
 */
public class ConnectionReusingTest {
    Logger logger=LoggerFactory.getLogger(ConnectionReusingTest.class);
    private ConnectionManager connectionManager=ConnectionManagerProvider.createConnectionManager(
            "adbcj:mysql://localhost/adbcjtck",
            "adbcjtck",
            "adbcjtck"
    );
    public Connection connection;
    private static final String alpha="abcdefghijklmnopqrstuvwxyz";
    private static final int MAX_INT=999999;
    private Random random=new Random();
    private int randInt(){
        return randInt(MAX_INT);
    }
    private int randInt(int num){
        return random.nextInt(num);
    }
    private char randChar(){
        return alpha.charAt(randInt()%26);
    }
    private String randString(int length){
        StringBuilder stringBuilder=new StringBuilder();
        for(int i=0;i<length;i++){
            stringBuilder.append(randChar());
        }
        return stringBuilder.toString();
    }




    private static final int CONN_NUM=20;
    private volatile int finishCount=0;
    private volatile int threadNum=0;
    @Test
    public void tryConnect(){
        try {
            connection=connectionManager.connect().get();
        }catch (Exception e){
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test(invocationCount = CONN_NUM,threadPoolSize = CONN_NUM, timeOut = 60000,dependsOnMethods = {"tryConnect"})
    public void reusedByNThreadsTest(){
        new Object(){
            String tablename=randString(7);
            int randint=randInt();
            int num=threadNum++;

            public void createTableIfNotExist(){
                try {
                    connection.executeUpdate("CREATE TABLE IF NOT EXISTS "+tablename+"(\n" +
                            "  id int(11) NOT NULL AUTO_INCREMENT,\n" +
                            "  name int(11) NOT NULL,\n" +
                            "  PRIMARY KEY (id)\n" +
                            ") ENGINE = INNODB;").addListener(new DbListener<Result>() {
                        @Override
                        public void onCompletion(DbFuture<Result> future) {
                            continueAndInsert();
                        }
                    });
                    logger.info(tablename + "-" + randint + " ---create table");
                }  catch (Exception e){
                    e.printStackTrace();
                    Assert.fail();
                }

            }
            public void continueAndInsert(){
                try {
                    connection.executeUpdate("INSERT into " + tablename + " (name) values (" + randint + ")")
                            .addListener(new DbListener<Result>() {
                                @Override
                                public void onCompletion(DbFuture<Result> future) {
                                    continueAndVerify();
                                }
                            });
                    logger.info(num+" : "+tablename + "-" + randint + " ---insert value");
                } catch (Exception e){
                    e.printStackTrace();
                    Assert.fail();
                }
            }
            public void continueAndVerify(){
                try {
                    logger.info(num+" : "+tablename+"-"+randint+" ---select start ");
                    connection.executeQuery("SELECT 1 from "+tablename+";");
                    connection.executeQuery("SELECT * from "+tablename+";").addListener(new DbListener<ResultSet>() {
                        @Override
                        public void onCompletion(DbFuture<ResultSet> future) {
                            try {
                                ResultSet rs = future.getResult();
                                Row r = rs.get(0);
                                int resultint = r.get(1).getInt();
                                logger.info(num+" : "+tablename + "-" + randint + " ---select got " + resultint);
                                finishWithDrop();
                                //org.testng.Assert.assertEquals(resultint,randint);

                            } catch (Exception e) {
                                e.printStackTrace();
                                Assert.fail();
                            }
                        }
                    });
                } catch (Exception e){
                    e.printStackTrace();
                    Assert.fail();
                }
            }
            public void finishWithDrop(){

                try {
                    logger.info(num+" : "+tablename+"-"+randint+" ---dropping");
                    connection.executeUpdate("drop table "+tablename+";").addListener(new DbListener<Result>() {
                        @Override
                        public void onCompletion(DbFuture<Result> future) {
                            finishCount++;
                        }
                    });
                }   catch (Exception e){
                    e.printStackTrace();
                    Assert.fail();
                }

            }
        }.createTableIfNotExist();
        try {
            Thread.sleep(30000);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    @Test(dependsOnMethods = {"reusedByNThreadsTest"})
    public void isAllThreadsPassedTest(){
        logger.info(finishCount+"");
        Assert.assertEquals(finishCount,CONN_NUM);

    }



}
