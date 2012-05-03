package org.adbcj.mysql.codec;

/**
 * @author roman.stoffel@gamlor.info
 * @since 12.04.12
 */
public final class Tuple<T1,T2> {
    private final T1 first;
    private final T2 second;


    public Tuple(T1 first, T2 second) {
        if(null==first){
            throw new IllegalArgumentException("Argument first cannot be null");
        }
        if(null==second){
            throw new IllegalArgumentException("Argument first cannot be null");
        }
        this.first = first;
        this.second = second;
    }

    public static  <T1,T2>  Tuple<T1,T2> create(T1 first, T2 second){
        return new Tuple<T1, T2>(first,second);
    }

    public T1 getFirst() {
        return first;
    }

    public T2 getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple tuple = (Tuple) o;

        if (!first.equals(tuple.first)) return false;
        if (!second.equals(tuple.second)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }
}
