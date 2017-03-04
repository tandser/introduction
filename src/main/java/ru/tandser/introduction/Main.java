package ru.tandser.introduction;

import javax.jms.DeliveryMode;
import javax.jms.Session;

public class Main {
    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer(Session.AUTO_ACKNOWLEDGE);
        Producer producer = new Producer(Session.AUTO_ACKNOWLEDGE, DeliveryMode.NON_PERSISTENT, 100);
        consumer.run();
        producer.run();
    }
}