package org.adbcj.h2;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.SecureRandom;

/**
 * @author roman.stoffel@gamlor.info
 */
final class MathUtils {
    /**
     * The secure random object.
     */
    volatile static SecureRandom cachedSecureRandom;

    /**
     * True if the secure random object is seeded.
     */
    static volatile boolean seeded;

    /**
     * Get a number of cryptographically secure pseudo random bytes.
     *
     * @param len the number of bytes
     * @return the random bytes
     */
    public static byte[] secureRandomBytes(int len) {
        if (len <= 0) {
            len = 1;
        }
        byte[] buff = new byte[len];
        SecureRandom sr = getSecureRandom();
        synchronized (sr) {
            sr.nextBytes(buff);
        }
        return buff;
    }

    private static synchronized SecureRandom getSecureRandom() {
        if (cachedSecureRandom != null) {
            return cachedSecureRandom;
        }
        // Workaround for SecureRandom problem as described in
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6202721
        // Can not do that in a static initializer block, because
        // threads are not started until after the initializer block exits
        try {
            cachedSecureRandom = SecureRandom.getInstance("SHA1PRNG");
            // On some systems, secureRandom.generateSeed() is very slow.
            // In this case it is initialized using our own seed implementation
            // and afterwards (in the thread) using the regular algorithm.
            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
                        byte[] seed = sr.generateSeed(20);
                        synchronized (cachedSecureRandom) {
                            cachedSecureRandom.setSeed(seed);
                            seeded = true;
                        }
                    } catch (Exception e) {
                        // NoSuchAlgorithmException
                        warn("SecureRandom", e);
                    }
                }
            };

            try {
                Thread t = new Thread(runnable, "Generate Seed");
                // let the process terminate even if generating the seed is really slow
                t.setDaemon(true);
                t.start();
                Thread.yield();
                try {
                    // normally, generateSeed takes less than 200 ms
                    t.join(400);
                } catch (InterruptedException e) {
                    warn("InterruptedException", e);
                }
                if (!seeded) {
                    byte[] seed = generateAlternativeSeed();
                    // this never reduces randomness
                    synchronized (cachedSecureRandom) {
                        cachedSecureRandom.setSeed(seed);
                    }
                }
            } catch (SecurityException e) {
                // workaround for the Google App Engine: don't use a thread
                runnable.run();
                generateAlternativeSeed();
            }

        } catch (Exception e) {
            // NoSuchAlgorithmException
            warn("SecureRandom", e);
            cachedSecureRandom = new SecureRandom();
        }
        return cachedSecureRandom;
    }

    /**
     * Print a message to system output if there was a problem initializing the
     * random number generator.
     *
     * @param s the message to print
     * @param t the stack trace
     */
    static void warn(String s, Throwable t) {
        // not a fatal problem, but maybe reduced security
        System.out.println("Warning: " + s);
        if (t != null) {
            t.printStackTrace();
        }
    }

    private static byte[] generateAlternativeSeed() {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);

            // milliseconds
            out.writeLong(System.currentTimeMillis());

            // nanoseconds if available
            try {
                Method m = System.class.getMethod("nanoTime");
                if (m != null) {
                    Object o = m.invoke(null);
                    out.writeUTF(o.toString());
                }
            } catch (Exception e) {
                // nanoTime not found, this is ok (only exists for JDK 1.5 and higher)
                out.writeUTF(e.toString());
            }

            // memory
            out.writeInt(new Object().hashCode());
            Runtime runtime = Runtime.getRuntime();
            out.writeLong(runtime.freeMemory());
            out.writeLong(runtime.maxMemory());
            out.writeLong(runtime.totalMemory());

            // environment
            try {
                String s = System.getProperties().toString();
                // can't use writeUTF, as the string
                // might be larger than 64 KB
                out.writeInt(s.length());
                out.write(s.getBytes("UTF-8"));
            } catch (Exception e) {
                warn("generateAlternativeSeed", e);
            }

            // host name and ip addresses (if any)
            try {
                // workaround for the Google App Engine: don't use InetAddress
                Class<?> inetAddressClass = Class.forName("java.net.InetAddress");
                Object localHost = inetAddressClass.getMethod("getLocalHost").invoke(null);
                String hostName = inetAddressClass.getMethod("getHostName").invoke(localHost).toString();
                out.writeUTF(hostName);
                Object[] list = (Object[]) inetAddressClass.getMethod("getAllByName", String.class).invoke(null, hostName);
                Method getAddress = inetAddressClass.getMethod("getAddress");
                for (Object o : list) {
                    out.write((byte[]) getAddress.invoke(o));
                }
            } catch (Throwable e) {
                // on some system, InetAddress is not supported
                // on some system, InetAddress.getLocalHost() doesn't work
                // for some reason (incorrect configuration)
            }

            // timing (a second thread is already running usually)
            for (int j = 0; j < 16; j++) {
                int i = 0;
                long end = System.currentTimeMillis();
                while (end == System.currentTimeMillis()) {
                    i++;
                }
                out.writeInt(i);
            }

            out.close();
            return bout.toByteArray();
        } catch (IOException e) {
            warn("generateAlternativeSeed", e);
            return new byte[1];
        }
    }
}
