package com.soyuan.steps.activemq;

import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;

import javax.jms.Connection;
import javax.jms.JMSException;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/25 上午10:19
 */
public class ActiveMQConsumer extends BaseStreamStep implements StepInterface {

    private static Class<?> PKG = ActiveMQConsumer.class;

    public ActiveMQConsumer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * 在调用init方法之前，会实例化ActiveMQConsumerMeta
     * 并调用meta.loadXML()初始化meta的成员变量
     * @param stepMetaInterface
     * @param stepDataInterface
     * @return
     */
    @Override
    public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) {
        ActiveMQConsumerMeta meta = (ActiveMQConsumerMeta) stepMetaInterface;
        ActiveMQConsumerData data = (ActiveMQConsumerData) stepDataInterface;
        if (!super.init(meta,data)){
            logError(BaseMessages.getString(PKG, "ActiveMQConsumer.Error.InitFailed"));
            return false;
        }
        try {
            //创建[行元数据]-即:输出哪些字段
            data.outputRowMeta = meta.getRowMeta(getStepname(), this);
        } catch (KettleStepException e) {
            log.logError(e.getMessage(), e);
        }

        //创建activemq connection
        final Connection connection;
        try {
            connection = ActiveMQFactory.getConn(meta.getActiveMQEntity());
            //subtransExecutor:子转换执行器
            window = new FixedTimeStreamWindow<>(
                    subtransExecutor,
                    data.outputRowMeta,
                    getDuration(),
                    getBatchSize());

            source = new ActiveMQStreamSource(connection, meta, data, this);
        } catch (JMSException e) {
            log.logError(e.getMessage(),e);
            return false;
        }
        return true;
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        super.dispose(smi, sdi);
    }
}
