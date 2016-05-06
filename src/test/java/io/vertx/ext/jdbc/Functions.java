package io.vertx.ext.jdbc;

public class Functions {

    public static int nap(int howLong) {
        try {
            Thread.sleep(howLong * 1000);
        } catch (InterruptedException e) {
            return -1;
        }
        return howLong;
    }
}
