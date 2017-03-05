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
 * API версии 1.1. Производитель сообщений может быть запущен как в
 * основном, так и в отдельном потоке исполнения.
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
                logger.error("There was an error at initial context initialization", exc);
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
                logger.error("There was an error at connection initialization", exc);
                throw new RuntimeException(exc);
            }
        }

        logger.info("Create {}", toString());
    }

    /**
     * Выполняет основную логику работы производителя по отправке
     * сообщений и протоколированию.
     */
    @Override
    public void run() {
        try {
            Destination destination = (Destination) context.get().lookup("MyQueue");
            Session session = connection.createSession(false, sessionMode);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(deliveryMode);
            String name = format("%s-%s", getClass().getSimpleName(), currentThread().getName());
            for (int i = 0; i < numberOfPosts; i++) {
                String text = format("Message %d from %s", i, name);
                TextMessage textMessage = session.createTextMessage(text);
                producer.send(textMessage);
                logger.info("Sent      : {}", format("%08x : %s", text.hashCode(), currentThread().getName()));
            }
        } catch (NamingException | JMSException exc) {
            logger.error("There was an error in work of the producer of messages", exc);
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
     * Возвращает строковое представление данного производителя
     * сообщений. Строковое представление состоит из простого имени
     * класса и заключённых в квадратные скобки и разделённых запятыми
     * режимов подтверждения доставки и стойкости сообщений, а также
     * количества сообщений, которое будет отправлено производителем.
     *
     * @return возвращает строкове представление данного производителя
     *         сообщений
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

        builder.append("delivery mode: ");

        switch (deliveryMode) {
            case 1  : builder.append("NON_PERSISTENT, ");
                      break;
            case 2  : builder.append("PERSISTENT, ");
                      break;
            default : builder.append("invalid");
        }

        builder.append("number of posts: ")
               .append(numberOfPosts)
               .append("]");

        return builder.toString();
    }
}