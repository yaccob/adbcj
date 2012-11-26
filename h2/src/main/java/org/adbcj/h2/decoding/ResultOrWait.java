package org.adbcj.h2.decoding;

/**
* @author roman.stoffel@gamlor.info
*/
class ResultOrWait<T>{
    final T result;
    final boolean couldReadResult;

    final static ResultOrWait WaitLonger = new ResultOrWait(null,false);
    final static ResultOrWait Start = new ResultOrWait(null,true);


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
