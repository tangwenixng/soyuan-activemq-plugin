package com.soyuan.steps.activemq;


import com.google.common.base.Preconditions;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.pentaho.di.core.util.StringUtil;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/25 下午2:34
 */
public class ActiveMQFactory {

    private static final Map<MessageConsumer, Connection> consumers = new HashMap<>();

    /**
     * 或许直接返回Connection比较好，就不用维护Map了
     * @param meta
     * @return
     * @throws JMSException
     */
    public static MessageConsumer consumer(ActiveMQConsumerMeta meta) throws JMSException{
        try {
            final Map<String, String> config = meta.getConfig();
            Preconditions.checkArgument(config.containsKey(ActiveMQConfig.USERNAME),"option must has username field");
            Preconditions.checkArgument(config.containsKey(ActiveMQConfig.PASSWORD),"option must has password field");

            final String username = config.get(ActiveMQConfig.USERNAME);
            final String password = config.get(ActiveMQConfig.PASSWORD);
            ConnectionFactory connectionFactory;
            if (!StringUtil.isEmpty(username) && !StringUtil.isEmpty(password)){
                 connectionFactory = new ActiveMQConnectionFactory(username,password,meta.getBrokerUrl());
            }else{
                connectionFactory = new ActiveMQConnectionFactory(meta.getBrokerUrl());
            }
            final Connection connection = connectionFactory.createConnection();
            connection.start();
            //3.创建session,第一个参数是是否使用事务，第二个参数是确认机制
            final Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            //4.创建目的地【消费者与生产者的目的地相同才能进行消息传递】
            Destination destination = session.createQueue(meta.getQueue());
            //5.创建消费者，第一个参数是目的地，此时创建的消费者要与目的地进行绑定。
            MessageConsumer consumer = session.createConsumer(destination);
            consumers.put(consumer, connection);
            return consumer;
        } catch (JMSException e) {
            throw  e;
        }
    }

    public static void closeConsumer(MessageConsumer consumer) {
        final Connection conn = consumers.getOrDefault(consumer, null);
        if (conn != null) {
            try {
                consumer.close();
                conn.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        consumers.remove(consumer);
    }

    public static Connection getConn(ActiveMQEntity entity) throws JMSException {
        try {
            final String username = entity.getUsername();
            final String password = entity.getPassword();
            ConnectionFactory connectionFactory;
            if (!StringUtil.isEmpty(username) && !StringUtil.isEmpty(password)) {
                connectionFactory = new ActiveMQConnectionFactory(username, password, entity.getBrokerUrl());
            } else {
                connectionFactory = new ActiveMQConnectionFactory(entity.getBrokerUrl());
            }
            final Connection connection = connectionFactory.createConnection();
            connection.start();

            return connection;
        } catch (JMSException e) {
            throw e;
        }
    }

    public static void closeConn(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }


}
