package com.soyuan.steps.activemq;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorData;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/25 上午10:20
 */
public class ActiveMQConsumerData extends TransExecutorData implements StepDataInterface {
    RowMetaInterface outputRowMeta;

    public ActiveMQConsumerData() {
        super();
    }
}
