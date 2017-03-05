package ru.tandser.introduction;

import javax.jms.DeliveryMode;
import javax.jms.Session;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer(Session.AUTO_ACKNOWLEDGE, 100);
        Producer producer = new Producer(Session.AUTO_ACKNOWLEDGE, DeliveryMode.NON_PERSISTENT, 100);
        new Thread(consumer).start();
        new Thread(producer).start();
        TimeUnit.SECONDS.sleep(3);
        Consumer.close();
        Producer.close();
    }
}