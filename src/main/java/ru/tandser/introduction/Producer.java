package ru.tandser.introduction;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;

public class Producer implements Runnable {

    private static ThreadLocal<Context> context = new ThreadLocal<Context>() {
        @Override
        protected Context initialValue() {
        try {
            return new InitialContext();
        } catch (NamingException exc) {
            throw new RuntimeException(exc);
        }
        }
    };

    private static Connection connection;

    private int sessionMode;
    private int deliveryMode;
    private int numberOfPosts;

    public Producer(int sessionMode, int deliveryMode, int numberOfPosts) {
        this.sessionMode   = sessionMode;
        this.deliveryMode  = deliveryMode;
        this.numberOfPosts = numberOfPosts;

        if (connection == null) {
            try {
                connection = ((ConnectionFactory) context.get().lookup("ConnectionFactory")).createConnection();
                connection.start();
            } catch (NamingException | JMSException exc) {
                throw new RuntimeException(exc);
            }

        }
    }

    @Override
    public void run() {
        Session session = null;
        try {
            Destination destination = (Destination) context.get().lookup("MyQueue");
            session = connection.createSession(false, sessionMode);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(deliveryMode);
            String name = String.format("%s-%s", getClass().getSimpleName(), currentThread().getName());
            for (int i = 0; i < numberOfPosts; i++) {
                String text = String.format("Message %d from %s", i, name);
                TextMessage textMessage = session.createTextMessage(text);
                producer.send(textMessage);
                System.out.println(format("Sent     : %08X : %s", text.hashCode(), name));
            }
        } catch (NamingException | JMSException exc) {
            throw new RuntimeException(exc);
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException exc) {
                    throw new RuntimeException(exc);
                }
            }
        }
    }

    public static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException exc) {
                throw new RuntimeException(exc);
            }
        }
    }
}