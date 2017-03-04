package ru.tandser.introduction;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;

public class Consumer implements Runnable, MessageListener {

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

    public static Connection connection;

    private int sessionMode;

    public Consumer(int sessionMode) {
        this.sessionMode = sessionMode;

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
        try {
            Destination destination = (Destination) context.get().lookup("MyQueue");
            Session session = connection.createSession(false, sessionMode);
            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(this);
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                String name = String.format("%s-%s", getClass().getSimpleName(), currentThread().getName());
                System.out.println(format("Received : %08X : %s", ((TextMessage) message).getText().hashCode(), name));
            }
            if (sessionMode == Session.CLIENT_ACKNOWLEDGE) {
                message.acknowledge();
            }
        } catch (JMSException exc) {
            throw new RuntimeException(exc);
        }
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException exc) {
                throw new RuntimeException(exc);
            }
        }
    }
}