package com.soyuan.steps.activemq;

import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import javax.jms.Connection;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/30 下午5:50
 */
public class ActiveMQProductData extends BaseStepData implements StepDataInterface {
    /**
     * 列msg在行中的位置
     */
    int msgFieldIndex;

    /**
     * MQ 连接
     */
    Connection conn;
}
