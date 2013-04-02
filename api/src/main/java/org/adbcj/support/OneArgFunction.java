package org.adbcj.support;


public interface OneArgFunction<TArgument, TReturn> {
    TReturn apply(TArgument arg);

    public static final OneArgFunction ID_FUNCTION = new OneArgFunction() {
        @Override
        public Object apply(Object arg) {
            return arg;
        }
    };
}


