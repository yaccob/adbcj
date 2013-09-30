package org.adbcj.tck.test;
import junit.framework.Assert;
import org.adbcj.*;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.stacktracing.StackTracingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author foooling@gmail.com
 */
public class ConnectionReusingTest extends AbstractWithConnectionManagerTest {
    Logger logger=LoggerFactory.getLogger(ConnectionReusingTest.class);
    private static final String alpha="abcdefghijklmnopqrstuvwxyz";
    private static final int MAX_INT=999999;
    private final Random random=new Random();
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
    private final AtomicInteger finishCount=new AtomicInteger();
    private final AtomicInteger threadNum=new AtomicInteger();



    @Test(invocationCount = CONN_NUM,threadPoolSize = CONN_NUM, timeOut = 60000)
    public void reusedByNThreadsTest() throws InterruptedException {

        final Connection connection =connectionManager.connect().get();
        DbFuture<Void> finalResult = new Object(){
            int randint=randInt();
            int num=threadNum.incrementAndGet();
            String tablename=randString(7)+"thread"+num;
            DefaultDbFuture<Void> overallResult = new DefaultDbFuture<Void>(StackTracingOptions.GLOBAL_DEFAULT);

            public DefaultDbFuture<Void> createTableIfNotExist(){
                connection.executeUpdate("CREATE TABLE IF NOT EXISTS "+tablename+"(\n" +
                        "  id int(11) NOT NULL AUTO_INCREMENT,\n" +
                        "  name int(11) NOT NULL,\n" +
                        "  PRIMARY KEY (id)\n" +
                        ")").addListener(new DbListener<Result>() {
                    @Override
                    public void onCompletion(DbFuture<Result> future) {
                        if(future.getState()==FutureState.SUCCESS){
                            continueAndInsert();
                        } else{
                            overallResult.trySetException(future.getException());
                        }
                    }
                });
                logger.info(tablename + "-" + randint + " ---create table");
                return overallResult;
            }
            public void continueAndInsert(){
                connection.executeUpdate("INSERT into " + tablename + " (name) values (" + randint + ")")
                    .addListener(new DbListener<Result>() {
                        @Override
                        public void onCompletion(DbFuture<Result> future) {
                            if(future.getState()==FutureState.SUCCESS){
                                continueAndVerify();
                            } else{
                                overallResult.trySetException(future.getException());
                            }
                        }
                    });
                logger.info(num+" : "+tablename + "-" + randint + " ---insert value");
            }
            public void continueAndVerify(){
                    logger.info(num+" : "+tablename+"-"+randint+" ---select start ");
                    connection.executeQuery("SELECT 1 from "+tablename+";");
                    connection.executeQuery("SELECT * from "+tablename+";").addListener(new DbListener<ResultSet>() {
                        @Override
                        public void onCompletion(DbFuture<ResultSet> future) {

                            if(future.getState()==FutureState.SUCCESS){
                                ResultSet rs = future.getResult();
                                Row r = rs.get(0);
                                int resultint = r.get(1).getInt();
                                logger.info(num+" : "+tablename + "-" + randint + " ---select got " + resultint);
                                finishWithDrop();
                                try{
                                    org.testng.Assert.assertEquals(resultint,randint);

                                } catch (AssertionError e){
                                    overallResult.trySetException(future.getException());
                                }
                            } else{
                                overallResult.trySetException(future.getException());
                            }
                        }
                    });
            }
            public void finishWithDrop(){
                logger.info(num+" : "+tablename+"-"+randint+" ---dropping");
                connection.executeUpdate("drop table "+tablename+";").addListener(new DbListener<Result>() {
                    @Override
                    public void onCompletion(DbFuture<Result> future) {

                        if(future.getState()==FutureState.SUCCESS){
                            finishCount.incrementAndGet();
                            overallResult.trySetResult(null);
                        } else{
                            overallResult.trySetException(future.getException());
                        }
                    }
                });

            }
        }.createTableIfNotExist();

        Assert.assertEquals(null,finalResult.get());
        try {
            Thread.sleep(30000);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    @Test(dependsOnMethods = {"reusedByNThreadsTest"})
    public void isAllThreadsPassedTest(){
        logger.info(finishCount+"");
        Assert.assertEquals(finishCount.get(),CONN_NUM);

    }



}
