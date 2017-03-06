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

/**
 * Этот класс представляет собой клиента, который потребляет
 * сообщения с помощью поставщика JMS, реализующего классический
 * API версии 1.1. Потребитель сообщений может быть запущен как в
 * основном, так и в отдельном потоке исполнения.
 *
 * @author Andrew Timokhin
 * @since  1.0
 */
public class Consumer implements Runnable, MessageListener {

    /**
     * Логгер для протоколирования. Конфигурация логгера располагается
     * в <i>classpath</i> / <i>logback.xml</i>.
     */
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    /**
     * Исходный JNDI-контекст для выполнения операций присвоения имён.
     * У каждого потока будет своя копия локальной переменной поскольку
     * <tt>Context</tt> не потокобезопасен.
     *
     * @see <a href="https://docs.oracle.com/javase/7/docs/api/javax/naming/Context.html">Context</a>
     */
    private static ThreadLocal<Context> context = new ThreadLocal<Context>() {
        @Override
        protected Context initialValue() {
            try {
                return new InitialContext();
            } catch (NamingException exc) {
                logger.error("There was an error at initial context initialization", exc);
                throw new RuntimeException(exc);
            }
        }
    };

    /**
     * Общее соединение с провайдером JMS для потребителей сообщений.
     * <tt>Connection</tt> является потокобезопасным и разделяемым в
     * целях снижения издержек на открытие нового соединения.
     *
     * @see <a href="https://docs.oracle.com/javaee/7/api/javax/jms/Connection.html">Connection</a>
     */
    private static Connection connection;

    /**
     * Атомарный неблокирующий счётчик для контроля количества
     * полученных сообщений. Инициализируется нулевым значением и
     * является общим для всех потоков потребителей сообщений.
     */
    private static AtomicInteger recieved = new AtomicInteger();

    /**
     * Простейший секундомер, для отсчёта продолжительности работы
     * потребителей сообщений. Не является потокобезопасным и может
     * выполнять только одну задачу в любой момент времени.
     *
     * @see StopWatch
     */
    private static StopWatch stopWatch = new StopWatch();

    /**
     * Режим подтверждения доставки сообщения. Инициализуруется
     * константой интерфейса <tt>Session</tt>.
     *
     * @see <a href="http://docs.oracle.com/javaee/7/api/javax/jms/Session.html#field.summary">Session</a>
     */
    private int sessionMode;

    /**
     * Ожидаемое для получения количество сообщений.
     */
    private int numberOfPosts;

    /**
     * Создаёт потребителя с указанными режимом подтверждения доставки
     * сообщений и ожидаемым для получения количеством сообщений.
     * Помимо этого выполняет первичную инициализацию общего
     * соединения.
     *
     * @param sessionMode   режим подтверждения доставки сообщения
     * @param numberOfPosts ожидаемое для получения количество
     *                      сообщений
     */
    public Consumer(int sessionMode, int numberOfPosts) {
        this.sessionMode   = sessionMode;
        this.numberOfPosts = numberOfPosts;

        if (connection == null) {
            try {
                connection = ((ConnectionFactory) context.get().lookup("ConnectionFactory")).createConnection();
                connection.start();
            } catch (NamingException | JMSException exc) {
                logger.error("There was an error at connection initialization", exc);
                throw new RuntimeException(exc);
            }
        }

        logger.info("Create {}", toString());
    }

    /**
     * Инициализирует потребителя сообщений и устанавливает его в
     * качестве слушателя сообщений для асинхронного приёма.
     */
    @Override
    public void run() {
        try {
            Destination destination = (Destination) context.get().lookup("MyQueue");
            Session session = connection.createSession(false, sessionMode);
            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(this);
        } catch (NamingException | JMSException exc) {
            logger.error("There was an error in work of the consumer of messages", exc);
            throw new RuntimeException(exc);
        }
    }

    /**
     * Реализует интерфейс слушателя сообщений и выполняет основную
     * логику работы потребителя по асинхронному получению сообщений и
     * протоколированию. Является методом обратного вызова и вызывается
     * автоматически при доставке сообщения от поставщика JMS.
     *
     * @param message доставленное сообщение
     */
    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                logger.info("Received  : {}", format("%08x : %s", ((TextMessage) message).getText().hashCode(), currentThread().getName()));
                if (sessionMode == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                }
                if (recieved.incrementAndGet() == numberOfPosts) {
                    logger.info("Stopwatch : {} ms", stopWatch.stop());
                }
            }
        } catch (JMSException exc) {
            logger.error("There was an error when obtaining the message", exc);
            throw new RuntimeException(exc);
        }
    }

    /**
     * Закрывает соединение и высвобождает все выделенные ресурсы.
     *
     * @see <a href="https://docs.oracle.com/javaee/7/api/javax/jms/Connection.html#close--">Connection.close()</a>
     */
    public static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException exc) {
                logger.error("There was an error when closing connection", exc);
                throw new RuntimeException(exc);
            }
        }
    }

    /**
     * Возвращает строковое представление данного потребителя
     * сообщений. Строковое представление состоит из простого имени
     * класса и заключённых в квадратные скобки и разделённых запятыми
     * режима подтверждения доставки сообщений и ожидаемого для
     * получения количества сообщений.
     *
     * @return строковое представление данного потребителя сообщений
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(getClass().getSimpleName())
                .append("[")
                .append("session mode: ");

        switch (sessionMode) {
            case 0  : builder.append("SESSION_TRANSACTED, ");
                      break;
            case 1  : builder.append("AUTO_ACKNOWLEDGE, ");
                      break;
            case 2  : builder.append("CLIENT_ACKNOWLEDGE, ");
                      break;
            case 3  : builder.append("DUPS_OK_ACKNOWLEDGE, ");
                      break;
            default : builder.append("invalid");
        }

        builder.append("number of posts: ")
                .append(numberOfPosts)
                .append("]");

        return builder.toString();
    }
}