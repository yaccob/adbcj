package org.adbcj.jdbc;

/**
 * @author roman.stoffel@gamlor.info
 * @since 30.03.12
 */
final class UncheckedThrow {
    private UncheckedThrow(){}

    public static RuntimeException throwUnchecked(final Throwable ex){
        // Now we use the 'generic' method. Normally the type T is inferred
        // from the parameters. However you can specify the type also explicit!
        // Now we du just that! We use the RuntimeException as type!
        // That means the throwsUnchecked throws an unchecked exception!
        // Since the types are erased, no type-information is there to prevent this!
        return UncheckedThrow.<RuntimeException>throwsUnchecked(ex);
    }

    /**
     * Remember, Generics are erased in Java. So this basically throws an Exception. The real
     * Type of T is lost during the compilation
     */
    public static <T extends Exception> T throwsUnchecked(Throwable toThrow) throws T{
        // Since the type is erased, this cast actually does nothing!!!
        // we can throw any exception
        throw (T) toThrow;
    }
}
