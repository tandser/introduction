package ru.tandser.introduction;

import javax.jms.DeliveryMode;
import javax.jms.Session;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer(Session.CLIENT_ACKNOWLEDGE, 100);
        Producer producer = new Producer(Session.CLIENT_ACKNOWLEDGE, DeliveryMode.NON_PERSISTENT, 100);
        Thread threadC = new Thread(consumer);
        Thread threadP = new Thread(producer);
        threadC.start();
        threadP.start();
        TimeUnit.SECONDS.sleep(10);
        Consumer.close();
        Producer.close();
    }
}