package ru.tandser.introduction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;

/**
 * Этот класс представляет собой клиента, который производит
 * сообщения с помощью поставщика JMS, реализующего классический
 * API версии 1.1.
 *
 * @author Andrew Timokhin
 * @since  1.0
 */
public class Producer implements Runnable {

    /**
     * Логгер для протоколирования. Конфигурация логгера располагается
     * в <i>classpath</i> / <i>logback.xml</i>.
     */
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

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
                throw new RuntimeException(exc);
            }
        }
    };

    /**
     * Общее соединение с провайдером JMS для производителей сообщений.
     * <tt>Connection</tt> является потокобезопасным и разделяемым в
     * целях снижения издержек на открытие нового соединения.
     *
     * @see <a href="https://docs.oracle.com/javaee/7/api/javax/jms/Connection.html">Connection</a>
     */
    private static Connection connection;

    /**
     * Режим подтверждения доставки сообщения. Инициализуруется
     * константой интерфейса <tt>Session</tt>.
     *
     * @see <a href="http://docs.oracle.com/javaee/7/api/javax/jms/Session.html#field.summary">Session</a>
     */
    private int sessionMode;

    /**
     * Режим доставки сообщения. Инициализируется константой интерфейса
     * <tt>DeliveryMode</tt>.
     *
     * @see <a href="https://docs.oracle.com/javaee/7/api/javax/jms/DeliveryMode.html#field.summary">DeliveryMode</a>
     */
    private int deliveryMode;

    /**
     * Количество сообщений, которое будет отправлено производителем.
     */
    private int numberOfPosts;

    /**
     * Создаёт производителя с указанными режимами подтверждения
     * доставки и стойкости сообщений, а также количеством сообщений,
     * которые необходимо отправить. Помимо этого выполняет первичную
     * инициализацию общего соединения.
     *
     * @param sessionMode   режим подтверждения доставки сообщения
     * @param deliveryMode  режим доставки сообщения
     * @param numberOfPosts количество сообщений, которое будет
     *                      отправлено производителем
     */
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

    /**
     *
     * Выполняет основную логику работы производителя по отправке
     * сообщений и протоколированию.
     */
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
                logger.info(format("Sent     : %08X : %s", text.hashCode(), name));
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

    /**
     * Закрывает соединение и высвобождает выделенные ресурсы.
     *
     * @see <a href="https://docs.oracle.com/javaee/7/api/javax/jms/Connection.html#close--">Connection.close()</a>
     */
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