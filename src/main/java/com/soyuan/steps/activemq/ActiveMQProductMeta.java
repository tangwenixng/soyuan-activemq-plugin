package com.soyuan.steps.activemq;

import com.google.common.base.Preconditions;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/30 下午5:45
 */
@Step(
        id = "ActiveMQProduct",
        name = "ActiveMQProduct.TypeLongDesc",
        description = "ActiveMQProduct.TypeTooltipDesc",
        image = "com/soyuan/steps/activemq/resources/activemq.svg",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming",
        i18nPackageName = "com.soyuan.steps.activemq",
        documentationUrl = "ActiveMQProduct.DocumentationURL",
        casesUrl = "ActiveMQProduct.CasesURL",
        forumUrl = "ActiveMQProduct.ForumURL"
)
@InjectionSupported(localizationPrefix = "ActiveMQProductMeta.Injection.")
public class ActiveMQProductMeta extends BaseStepMeta implements StepMetaInterface {

    public static final String XML_BLANK = "    ";
    /**
     * 以下静态变量用于定义xml中的标签tag
     */
    public static final String BROKER_URL = "brokerUrl";
    public static final String QUEUE_NAME = "queue";
    public static final String MSG_FIELD = "msgField";
    private static final String ADVANCED_CONFIG = "advancedConfig";
    private static final String CONFIG_OPTION = "option";
    private static final String OPTION_PROPERTY = "property";
    private static final String OPTION_VALUE = "value";
    /**
     * 注入的配置: 注意是transient
     * 在哪赋值的-Dialog中
     */
    @Injection(name = "NAMES", group = "CONFIGURATION_PROPERTIES")
    protected transient List<String> injectedConfigNames;
    @Injection(name = "VALUES", group = "CONFIGURATION_PROPERTIES")
    protected transient List<String> injectedConfigValues;
    /**
     * 连接地址
     */
    @Injection(name = "BROKER_URL")
    private String brokerUrl;
    /**
     * 队列名称
     */
    @Injection(name = "QUEUE")
    private String queue;
    /**
     * 发送的字段
     */
    @Injection(name = "MSG")
    private String msgField;
    /**
     * 存放xml 中的advancedConfig option
     */
    private Map<String, String> config = new LinkedHashMap<>();


    public ActiveMQProductMeta() {
        //不要忘记调用父类初始化
        super();
    }

    @Override
    public void setDefault() {

    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new ActiveMQProduct(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new ActiveMQProductData();
    }

    public Map<String, String> getConfig() {
        applyInjectedProperties();
        return config;
    }

    public void setConfig(Map<String, String> advancedConfig) {
        this.config = advancedConfig;
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder sb = new StringBuilder();
        sb.append(XML_BLANK).append(XMLHandler.addTagValue(BROKER_URL, brokerUrl));
        sb.append(XML_BLANK).append(XMLHandler.addTagValue(QUEUE_NAME, queue));
        sb.append(XML_BLANK).append(XMLHandler.addTagValue(MSG_FIELD, msgField));

        sb.append(XML_BLANK).append(XMLHandler.openTag(ADVANCED_CONFIG)).append(Const.CR);
        getConfig().forEach((key, value) -> sb.append(XML_BLANK).append(XML_BLANK)
                .append(XMLHandler.addTagValue(CONFIG_OPTION, "", true,
                        OPTION_PROPERTY, (String) key, OPTION_VALUE, (String) value)));
        sb.append(XML_BLANK).append(XMLHandler.closeTag(ADVANCED_CONFIG)).append(Const.CR);
        return sb.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        setBrokerUrl(XMLHandler.getTagValue(stepnode, BROKER_URL));
        setQueue(XMLHandler.getTagValue(stepnode, QUEUE_NAME));
        setMsgField(XMLHandler.getTagValue(stepnode, MSG_FIELD));

        final Node advancedConfig = XMLHandler.getSubNode(stepnode, ADVANCED_CONFIG);
        if (advancedConfig != null) {
            final NodeList configItems = advancedConfig.getChildNodes();
            for (int i = 0; i < configItems.getLength(); i++) {
                final Node node = configItems.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    //如果节点是option
                    if (CONFIG_OPTION.equals(node.getNodeName())) {
                        config.put(node.getAttributes().getNamedItem(OPTION_PROPERTY).getTextContent(),
                                node.getAttributes().getNamedItem(OPTION_VALUE).getTextContent());
                    } else {
                        config.put(node.getNodeName(), node.getTextContent());
                    }
                }
            }
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        rep.saveStepAttribute(id_transformation, id_step, BROKER_URL, brokerUrl);
        rep.saveStepAttribute(id_transformation, id_step, QUEUE_NAME, queue);
        rep.saveStepAttribute(id_transformation, id_step, MSG_FIELD, MSG_FIELD);

        //保存config
        rep.saveStepAttribute(id_transformation, id_step, ADVANCED_CONFIG + "_COUNT", getConfig().size());
        int i = 0;
        for (String propName : getConfig().keySet()) {
            rep.saveStepAttribute(id_transformation, id_step, i, ADVANCED_CONFIG + "_PROP", propName);
            rep.saveStepAttribute(id_transformation, id_step, i++, ADVANCED_CONFIG + "_VALUE", getConfig().get(propName));
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        setBrokerUrl(rep.getStepAttributeString(id_step, BROKER_URL));
        setQueue(rep.getStepAttributeString(id_step, QUEUE_NAME));
        setMsgField(rep.getStepAttributeString(id_step, MSG_FIELD));

        config = new LinkedHashMap<>();
        long configSize = rep.getStepAttributeInteger(id_step, ADVANCED_CONFIG + "_COUNT");
        for (int i = 0; i < configSize; i++) {
            config.put(rep.getStepAttributeString(id_step, i, ADVANCED_CONFIG + "_PROP"),
                    rep.getStepAttributeString(id_step, i, ADVANCED_CONFIG + "_VALUE"));
        }
    }

    public ActiveMQEntity getActiveMQEntity() {
        return new ActiveMQEntity(brokerUrl,
                queue,
                config.get(ActiveMQConfig.USERNAME),
                config.get(ActiveMQConfig.PASSWORD));
    }

    private void applyInjectedProperties() {
        if (injectedConfigNames != null || injectedConfigValues != null) {
            Preconditions.checkState(injectedConfigNames != null, "Options names were not injected");
            Preconditions.checkState(injectedConfigValues != null, "Options values were not injected");
            Preconditions.checkState(injectedConfigNames.size() == injectedConfigValues.size(),
                    "Injected different number of options names and value");

            for (int i = 0; i < injectedConfigNames.size(); i++) {
                config.put(injectedConfigNames.get(i), injectedConfigValues.get(i));
            }
            //这里为什么要重新置null
            injectedConfigNames = null;
            injectedConfigValues = null;
        }
    }


    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getMsgField() {
        return msgField;
    }

    public void setMsgField(String msgField) {
        this.msgField = msgField;
    }
}
