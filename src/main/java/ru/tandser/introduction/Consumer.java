package ru.tandser.introduction;

import org.apache.activemq.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;

public class Consumer implements Runnable, MessageListener {

    /**
     * Логгер для протоколирования. Конфигурация логгера
     * располагается в <i>classpath</i> / <i>logback.xml</i>.
     */
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    /**
     * Исходный JNDI-контекст для выполнения операций
     * присвоения имён. У каждого потока будет своя
     * копия локальной переменной поскольку {@link Context}
     * не потокобезопасен.
     */
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

    /**
     * Общее соединение для потребителей сообщений.
     * {@link Connection} поддерживает одновременное
     * использование.
     */
    private static Connection connection;

    private static AtomicInteger recieved = new AtomicInteger();

    private static StopWatch stopWatch = new StopWatch();

    private int sessionMode;
    private int numberOfPosts;

    public Consumer(int sessionMode, int numberOfPosts) {
        this.sessionMode   = sessionMode;
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
                logger.info(format("Received : %08X : %s", ((TextMessage) message).getText().hashCode(), name));
                if (sessionMode == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                }
                if (recieved.incrementAndGet() == numberOfPosts) {
                    System.out.println(stopWatch.stop());
                }
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