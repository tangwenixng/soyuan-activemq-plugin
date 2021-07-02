package com.soyuan.steps.activemq;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/6/25 上午10:22
 */
@Step(
        id = "ActiveMQConsumer",
        name = "ActiveMQConsumer.TypeLongDesc",
        description = "ActiveMQConsumer.TypeTooltipDesc",
        image = "com/soyuan/steps/activemq/resources/activemq.svg",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Streaming",
        i18nPackageName = "com.soyuan.steps.activemq",
        documentationUrl = "ActiveMQConsumer.DocumentationURL",
        casesUrl = "ActiveMQConsumer.CasesURL",
        forumUrl = "ActiveMQConsumer.ForumURL"
)
@InjectionSupported(localizationPrefix = "ActiveMQConsumerMeta.Injection.")
public class ActiveMQConsumerMeta extends BaseStreamStepMeta implements StepMetaInterface {

    private static Class<?> PKG = ActiveMQConsumerMeta.class;

    /**
     * 以下静态变量用于定义xml中的标签tag
     */
    public static final String BROKER_URL = "brokerUrl";
    public static final String QUEUE_NAME = "queue";

    public static final String TRANSFORMATION_PATH = "transformationPath";
    public static final String BATCH_SIZE = "batchSize";
    public static final String BATCH_DURATION = "batchDuration";

    public static final String OUTPUT_FIELD_TAG_NAME = "OutputField";
    public static final String INPUT_NAME_ATTRIBUTE = "input";
    public static final String TYPE_ATTRIBUTE = "type";

    public static final String ADVANCED_CONFIG = "advancedConfig" ;
    private static final String CONFIG_OPTION = "option";
    private static final String OPTION_PROPERTY = "property";
    private static final String OPTION_VALUE = "value";


    /**
     * 连接地址
     */
    @Injection( name = "BROKER_URL" )
    private String brokerUrl;

    /**
     * 队列名称
     */
    @Injection(name="QUEUE")
    private String queue;

    /**
     * 注入的配置: 注意是transient
     * 在哪赋值的-Dialog中
     */
    @Injection(name = "NAMES", group = "CONFIGURATION_PROPERTIES")
    protected transient List<String> injectedConfigNames;

    @Injection(name = "VALUES", group = "CONFIGURATION_PROPERTIES")
    protected transient List<String> injectedConfigValues;

    private ActiveMQConsumerField msgIdField;
    private ActiveMQConsumerField msgField;
    private ActiveMQConsumerField timestampField;

    /**
     * 存放xml 中的advancedConfig option
     */
    private Map<String, String> config = new LinkedHashMap<>();


    public ActiveMQConsumerMeta() {
        super();
        msgIdField = new ActiveMQConsumerField(
                ActiveMQConsumerField.Name.MESSAGEID,
                "MessageId");
        msgField = new ActiveMQConsumerField(
                ActiveMQConsumerField.Name.MESSAGE,
                "Message"
        );
        timestampField = new ActiveMQConsumerField(
                ActiveMQConsumerField.Name.TIMESTAMP,
                "Timestamp",
                ActiveMQConsumerField.Type.Integer
        );
        setSpecificationMethod(ObjectLocationSpecificationMethod.FILENAME);
    }

    @Override
    public RowMeta getRowMeta(String origin, VariableSpace space) throws KettleStepException {
        RowMeta rowMeta = new RowMeta();
        putFieldOnRowMeta(getMsgIdField(), rowMeta, origin, space);
        putFieldOnRowMeta(getMsgField(), rowMeta, origin, space);
        putFieldOnRowMeta(getTimestampField(), rowMeta, origin, space);
        return rowMeta;
    }

    private void putFieldOnRowMeta(ActiveMQConsumerField field, RowMetaInterface rowMeta,
                                   String origin, VariableSpace space) throws KettleStepException {
        if (field != null && !Utils.isEmpty(field.getOutputName())) {
            try {
                String value = space.environmentSubstitute(field.getOutputName());
                ValueMetaInterface v = ValueMetaFactory.createValueMeta(value,
                        field.getOutputType().getValueMetaInterfaceType());
                //这里为什么要set步骤名称
                v.setOrigin(origin);
                rowMeta.addValueMeta(v);
            } catch (KettlePluginException e) {
                throw new KettleStepException(BaseMessages.getString(
                        PKG,
                        "ActiveMQConsumerInputMeta.UnableToCreateValueType",
                        field
                ), e);
            }
        }
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new ActiveMQConsumer(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new ActiveMQConsumerData();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        //找到子级ktr路径
        setTransformationPath(XMLHandler.getTagValue(stepnode, TRANSFORMATION_PATH));
        //fileName(StepWithMappingMeta属性)为什么要设置成子级ktr路径
        setFileName(XMLHandler.getTagValue(stepnode, TRANSFORMATION_PATH));
        setBatchSize(XMLHandler.getTagValue(stepnode, BATCH_SIZE));
        setBatchDuration(XMLHandler.getTagValue(stepnode, BATCH_DURATION));
        //找到SUB_STEP 标签
        String subStepTag = XMLHandler.getTagValue(stepnode, SUB_STEP);
        if (!StringUtil.isEmpty(subStepTag)) {
            setSubStep(subStepTag);
        }

        setBrokerUrl(XMLHandler.getTagValue(stepnode, BROKER_URL));
        setQueue(XMLHandler.getTagValue(stepnode, QUEUE_NAME));

        //找到输出字段OutputField tag
        List<Node> ofNode = XMLHandler.getNodes(stepnode, OUTPUT_FIELD_TAG_NAME);
        ofNode.forEach(node -> {
            String outputName = XMLHandler.getNodeValue(node);
            String inputName = XMLHandler.getTagAttribute(node, INPUT_NAME_ATTRIBUTE);
            String type = XMLHandler.getTagAttribute(node, TYPE_ATTRIBUTE);
            ActiveMQConsumerField field = new ActiveMQConsumerField(
                    ActiveMQConsumerField.Name.valueOf(inputName.toUpperCase()),
                    outputName,
                    ActiveMQConsumerField.Type.valueOf(type)
            );
            setField(field);
        });

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
    public String getXML() {
        StringBuilder sb = new StringBuilder();
        sb.append("    ").append(XMLHandler.addTagValue(TRANSFORMATION_PATH, transformationPath));
        sb.append("    ").append(XMLHandler.addTagValue(SUB_STEP, getSubStep()));
        sb.append("    ").append(XMLHandler.addTagValue(BATCH_SIZE, batchSize));
        sb.append("    ").append(XMLHandler.addTagValue(BATCH_DURATION, batchDuration));

        sb.append("    ").append(XMLHandler.addTagValue(BROKER_URL, brokerUrl));
        sb.append("    ").append(XMLHandler.addTagValue(QUEUE_NAME, queue));

        //fields属性
        getFieldDefinitions().forEach(field -> sb.append("    ")
                .append(XMLHandler.addTagValue(
                        OUTPUT_FIELD_TAG_NAME,
                        field.getOutputName(),
                        true,
                        INPUT_NAME_ATTRIBUTE, field.getInputName().toString(),
                        TYPE_ATTRIBUTE, field.getOutputType().toString()
                )));

        //将高级配置写到xml中
        sb.append("    ").append(XMLHandler.openTag(ADVANCED_CONFIG)).append(Const.CR);
        getConfig().forEach((key, value) -> sb.append("        ")
                .append(XMLHandler.addTagValue(CONFIG_OPTION, "", true,
                        OPTION_PROPERTY, (String) key, OPTION_VALUE, (String) value)));
        sb.append("    ").append(XMLHandler.closeTag(ADVANCED_CONFIG)).append(Const.CR);
        return sb.toString();
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId) throws KettleException {
        rep.saveStepAttribute(transId, stepId, TRANSFORMATION_PATH, transformationPath);
        rep.saveStepAttribute(transId, stepId, SUB_STEP, getSubStep());
        rep.saveStepAttribute(transId, stepId, BATCH_SIZE, batchSize);
        rep.saveStepAttribute(transId, stepId, BATCH_DURATION, batchDuration);


        rep.saveStepAttribute(transId, stepId, BROKER_URL, brokerUrl);
        rep.saveStepAttribute(transId, stepId, QUEUE_NAME, queue);

        //保存fields
        final List<ActiveMQConsumerField> fields = getFieldDefinitions();
        for (ActiveMQConsumerField field : fields) {
            String prefix = OUTPUT_FIELD_TAG_NAME + "_" + field.getInputName().toString();
            //值-输出值
            rep.saveStepAttribute(transId, stepId, prefix, field.getOutputName());
            rep.saveStepAttribute(transId, stepId, prefix + "_" + TYPE_ATTRIBUTE, field.getOutputType().toString());
        }

        //保存config
        rep.saveStepAttribute(transId, stepId, ADVANCED_CONFIG + "_COUNT", getConfig().size());
        int i = 0;
        for (String propName : getConfig().keySet()) {
            rep.saveStepAttribute(transId, stepId, i, ADVANCED_CONFIG + "_PROP", propName);
            rep.saveStepAttribute(transId, stepId, i++, ADVANCED_CONFIG + "_VALUE", getConfig().get(propName));
        }

    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        setFileName(rep.getStepAttributeString(id_step,TRANSFORMATION_PATH));
        setBatchSize(rep.getStepAttributeString(id_step, BATCH_SIZE));
        setBatchDuration(rep.getStepAttributeString(id_step, BATCH_DURATION));
        //找到SUB_STEP 标签
        String subStepTag = rep.getStepAttributeString(id_step, SUB_STEP);
        if (!StringUtil.isEmpty(subStepTag)) {
            setSubStep(subStepTag);
        }

        setBrokerUrl(rep.getStepAttributeString(id_step, BROKER_URL));
        setQueue(rep.getStepAttributeString(id_step, QUEUE_NAME));

        //读取fields
        for (ActiveMQConsumerField.Name name : ActiveMQConsumerField.Name.values()) {
            String prefix = OUTPUT_FIELD_TAG_NAME + "_" + name;
            String output = rep.getStepAttributeString(id_step, prefix);
            String type = rep.getStepAttributeString(id_step, prefix + "_" + TYPE_ATTRIBUTE);
            if (output != null) {
                setField(new ActiveMQConsumerField(name, output, ActiveMQConsumerField.Type.valueOf(type)));
            }
        }

        //读取config
        config = new LinkedHashMap<>();
        for (int i = 0; i < rep.getStepAttributeInteger(id_step, ADVANCED_CONFIG + "_COUNT"); i++) {
            config.put(rep.getStepAttributeString(id_step, i, ADVANCED_CONFIG + "_PROP"),
                    rep.getStepAttributeString(id_step, i, ADVANCED_CONFIG + "_VALUE"));
        }
    }

    public Map<String, String> getConfig() {
        //解析自定义配置
        applyInjectedProperties();
        return config;
    }

    private void applyInjectedProperties() {
        if ( injectedConfigNames != null || injectedConfigValues != null ) {
            Preconditions.checkState( injectedConfigNames != null, "Options names were not injected" );
            Preconditions.checkState( injectedConfigValues != null, "Options values were not injected" );
            Preconditions.checkState( injectedConfigNames.size() == injectedConfigValues.size(),
                    "Injected different number of options names and value" );

            for (int i = 0; i < injectedConfigNames.size(); i++) {
                config.put(injectedConfigNames.get(i), injectedConfigValues.get(i));
            }
            //这里为什么要重新置null
            injectedConfigNames = null;
            injectedConfigValues = null;
        }
    }

    public List<ActiveMQConsumerField> getFieldDefinitions() {
        return Lists.newArrayList(getMsgIdField(), getMsgField(), getTimestampField());
    }

    protected void setField(ActiveMQConsumerField field) {
        field.getInputName().setFieldOnMeta(this, field);
    }

    public ActiveMQEntity getActiveMQEntity() {
        return new ActiveMQEntity(brokerUrl,
                queue,
                config.get(ActiveMQConfig.USERNAME),
                config.get(ActiveMQConfig.PASSWORD));
    }

    @Override
    public void setDefault() {
        super.setDefault();
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

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public ActiveMQConsumerField getMsgIdField() {
        return msgIdField;
    }

    public void setMsgIdField(ActiveMQConsumerField msgIdField) {
        this.msgIdField = msgIdField;
    }

    public ActiveMQConsumerField getMsgField() {
        return msgField;
    }

    public void setMsgField(ActiveMQConsumerField msgField) {
        this.msgField = msgField;
    }

    public ActiveMQConsumerField getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(ActiveMQConsumerField timestampField) {
        this.timestampField = timestampField;
    }

    public List<String> getInjectedConfigNames() {
        return injectedConfigNames;
    }

    public void setInjectedConfigNames(List<String> injectedConfigNames) {
        this.injectedConfigNames = injectedConfigNames;
    }

    public List<String> getInjectedConfigValues() {
        return injectedConfigValues;
    }

    public void setInjectedConfigValues(List<String> injectedConfigValues) {
        this.injectedConfigValues = injectedConfigValues;
    }

}
