package com.soyuan.steps.activemq;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import javax.jms.*;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/30 下午5:44
 */
public class ActiveMQProduct extends BaseStep implements StepInterface {
    private static final Class<?> PKG = ActiveMQProduct.class;
    private ActiveMQProductMeta meta;
    private ActiveMQProductData data;

    public ActiveMQProduct(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        super.init(smi, sdi);
        meta = (ActiveMQProductMeta) smi;
        data = (ActiveMQProductData) sdi;
        return true;
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        Object[] r = getRow();
        if (r == null) {
            ActiveMQFactory.closeConn(data.conn);
            setOutputDone();
            return false;
        }
        if (first) {
            data.msgFieldIndex = getInputRowMeta().indexOfValue(environmentSubstitute(meta.getMsgField()));
            try {
                data.conn = ActiveMQFactory.getConn(meta.getActiveMQEntity());
            } catch (JMSException e) {
                log.logError(e.getMessage(), e);
                setOutputDone();
                return false;
            }
            first = false;
        }

        //成功获取到了连接
        try {
            Connection conn = data.conn;
            final Session session = conn.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(meta.getQueue());
            MessageProducer producer = session.createProducer(destination);
            //maybe throw ClassCastException
            String content = (String) r[data.msgFieldIndex];
            TextMessage msg = session.createTextMessage(content);
            producer.send(msg);

            session.commit();

            incrementLinesOutput();

            putRow(getInputRowMeta(), r);

            if (checkFeedback(getLinesRead())) {
                if (log.isBasic()) {
                    logBasic(BaseMessages.getString(PKG, "ActiveMQProduct.Log.LineNumber") + getLinesRead());
                }
            }
            return true;
        } catch (Exception e) {
            log.logError(e.getMessage(), e);
            ActiveMQFactory.closeConn(data.conn);
            setOutputDone();
            return false;
        }
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
    }
}
