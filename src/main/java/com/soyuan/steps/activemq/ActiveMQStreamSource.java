package com.soyuan.steps.activemq;

import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/25 上午11:38
 */
public class ActiveMQStreamSource extends BlockingQueueStreamSource<List<Object>> {

    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private ActiveMQConsumerMeta consumerMeta;
    private ActiveMQConsumerData consumerData;
    private Connection connection;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private ActiveMQConsumerCallable callable;
    private Future<Void> future;
    private HashMap<ActiveMQConsumerField.Name, Integer> positions;

    protected ActiveMQStreamSource(Connection connection, ActiveMQConsumerMeta consumerMeta,
                                   ActiveMQConsumerData consumerData, ActiveMQConsumer consumerStep) {
        super(consumerStep);
        this.consumerMeta = consumerMeta;
        this.consumerData = consumerData;
        this.connection = connection;
    }

    @Override
    public void close() {
        callable.shutdown();
    }

    @Override
    public void open() {
        if (future != null) {
            logger.warn("open() called more than once");
            return;
        }
        //目的是找出: 某一列的位置
        //比如：Message-1  MessageId-2
        final List<ValueMetaInterface> valueMetas = consumerData.outputRowMeta.getValueMetaList();
        positions = new HashMap<>(valueMetas.size());

        for (int i = 0; i < valueMetas.size(); i++) {
            for (ActiveMQConsumerField.Name name : ActiveMQConsumerField.Name.values()) {
                final ActiveMQConsumerField field = name.getFieldFromMeta(consumerMeta);
                String outputName = field.getOutputName();
                if (outputName != null && outputName.equals(valueMetas.get(i).getName())) {
                    positions.putIfAbsent(name, i);
                }
            }
        }

        callable = new ActiveMQConsumerCallable(connection, super::close);
        future = executorService.submit(callable);
    }

    class ActiveMQConsumerCallable implements Callable<Void> {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Connection connection;
        private final Runnable onClose;

        ActiveMQConsumerCallable(Connection connection, Runnable onClose) {
            this.connection = connection;
            this.onClose = onClose;
        }

        @Override
        public Void call() throws JMSException {
            try {
                Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
                Queue queue = session.createQueue(consumerMeta.getQueue());
                MessageConsumer consumer = session.createConsumer(queue);

                while (!closed.get()) {
                    final TextMessage msg = (TextMessage) consumer.receive(1000L);
                    if (msg != null) {
                        List<List<Object>> rows = new ArrayList<>(1);

                        final List<Object> row = processMessageAsRow(msg);
                        rows.add(row);

                        acceptRows(rows);

                        session.commit();
                    }
                }
                return null;
            } catch (JMSException e) {
                if (!closed.get()){
                    throw e;
                }
                return null;
            } finally {
                ActiveMQFactory.closeConn(connection);
                onClose.run();
            }
        }

        public void shutdown() {
            //外部调用shutdown会触发while停止
            //从而让consumer停止，间接会调用父类的close
            closed.set(true);
        }
    }

    List<Object> processMessageAsRow(TextMessage msg) throws JMSException {
        Object[] rowData = RowDataUtil.allocateRowData(consumerData.outputRowMeta.size());

        if (positions.get(ActiveMQConsumerField.Name.MESSAGEID) != null) {
            rowData[positions.get(ActiveMQConsumerField.Name.MESSAGEID)] = msg.getJMSMessageID();
        }

        if (positions.get(ActiveMQConsumerField.Name.MESSAGE) != null) {
            rowData[positions.get(ActiveMQConsumerField.Name.MESSAGE)] = msg.getText();
        }

        if (positions.get(ActiveMQConsumerField.Name.TIMESTAMP) != null) {
            rowData[positions.get(ActiveMQConsumerField.Name.TIMESTAMP)] = msg.getJMSTimestamp();
        }

        return Arrays.asList(rowData);
    }
}
