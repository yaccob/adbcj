package org.adbcj.h2.decoding;


import org.adbcj.Value;

public final class ResultOrWait<T>{
    public final T result;
    public final boolean couldReadResult;

    //    @SuppressWarnings("unchecked")
    //    @SuppressWarnings("unchecked")
//    public final static ResultOrWait Start = new ResultOrWait(null,true);
//    public final static ResultOrWait WaitLonger = new ResultOrWait(null,false);

    public final static ResultOrWait<Value> StartWaitValue = new ResultOrWait<>(null,true);
    public final static ResultOrWait<Value> WaitLongerValue = new ResultOrWait<>(null,false);

    public final static ResultOrWait<Boolean> StartWaitBoolean = new ResultOrWait<>(null,true);
    public final static ResultOrWait<Boolean> WaitLongerBoolean = new ResultOrWait<>(null,false);

    public final static ResultOrWait<Integer> StartWaitInteger = new ResultOrWait<>(null,true);
    public final static ResultOrWait<Integer> WaitLongerInteger = new ResultOrWait<>(null,false);

    public final static ResultOrWait<Long> StartWaitLong = new ResultOrWait<>(null,true);
    public final static ResultOrWait<Long> WaitLongerLong = new ResultOrWait<>(null,false);

    public final static ResultOrWait<Double> StartWaitDouble = new ResultOrWait<>(null,true);
    public final static ResultOrWait<Double> WaitLongerDouble = new ResultOrWait<>(null,false);

    public final static ResultOrWait<String> StartWaitString = new ResultOrWait<>(null,true);
    public final static ResultOrWait<String> WaitLongerString = new ResultOrWait<>(null,false);

    public final static ResultOrWait<byte[]> StartWaitByteArray = new ResultOrWait<>(null,true);
    public final static ResultOrWait<byte[]> WaitLongerByteArray = new ResultOrWait<>(null,false);

    ResultOrWait(T result, boolean couldReadResult) {
        this.result = result;
        this.couldReadResult = couldReadResult;
    }

    public static <T> ResultOrWait<T> result(T data) {
        return new ResultOrWait<T>(data,true);
    }

    @Override
    public String toString() {
        if(couldReadResult){
            return String.valueOf(result);
        }else{
            return "{No Result}";
        }
    }
}
