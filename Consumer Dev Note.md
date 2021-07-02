# ActiveMQ Consumer组件开发笔记

## 概览

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702151314.png)

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702151330.png)

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702151359.png)



## 前提

该插件基于`kettle 8.1.0.0-365` 开发

如果是其他版本，不保证可用。(由于继承的`BaseStreamingDialog`等父类会随版本而变化)

> 本插件模仿官方Kafka插件源码编写：
>
> https://github.com/pentaho/big-data-plugin/tree/master/kettle-plugins/kafka



暂不支持topic，需要的可自行修改源码(工程量应该不大)。

## 必备模板

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702104206.png)

首先必须创建的4个类：

- ActiveMQConsumer extends BaseStreamStep implements StepInterface
- ActiveMQConsumerData extends TransExecutorData implements StepDataInterface
- ActiveMQConsumerDialog extends BaseStreamingDialog implements StepDialogInterface
- ActiveMQConsumerMeta extends BaseStreamStepMeta implements StepMetaInterface

注意这4个类继承的父类比较特殊，不同于一般的**步骤插件**继承的是`BaseStep***`  



然后创建多语言(资源)配置文件：结构如下图所示

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702105051.png)



接下来将分别说明刚刚列举的4个类。



## ActiveMQConsumerMeta

`ActiveMQConsumerMeta`是非常重要的一个类。

1. 可视化Dialog里看到的属性值(比如: Text框框)在点击了**确认**按钮时会保存到`ActiveMQConsumerMeta`中对应的成员变量的。当第一次打开步骤界面Dialog时(即open方法时-后面会讲到),也是从`ActiveMQConsumerMeta`中读取成员变量赋值到Text框框中。
2. 当在Kettle编辑界面点击了**保存Save**按钮时，会将`ActiveMQConsumerMeta`中的属性通过`getXML()`方法写入到文件(ktr)中。当点击运行按钮时，kettle会调用`loadXML()`将ktr文件内容读取到`ActiveMQConsumerMeta`成员变量中。同理readRep和saveRep。

上面介绍了Meta类的主要工作，接着具体说明下代码中需要注意的点：

### Step注解

```java
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
```

@step注解是定义步骤的规范，kettle会自动扫描此注解，并将它注入到插件容器内。

- id必须是全局唯一的
- name: 也就是我们在可视化界面中看到的插件名字。后面跟的`ActiveMQConsumer.TypeLongDesc`指向的是配置文件properties中的属性
- `@InjectionSupported(localizationPrefix = "ActiveMQConsumerMeta.Injection.")` 中的`ActiveMQConsumerMeta.Injection.`需要配合`ActiveMQConsumerMeta`中的成员变量来使用。比如：

```java
/**
     * 连接地址
     */
@Injection( name = "BROKER_URL" )
private String brokerUrl;
```

这里的`BROKER_URL`和刚刚的`ActiveMQConsumerMeta.Injection.`搭配起来就成了`ActiveMQConsumer.Injection.BROKER_URL`。

这个属性也是在配置文件properties中配置的



### 构造方法

```java
public ActiveMQConsumerMeta() {
  super();
  ...
  setSpecificationMethod(ObjectLocationSpecificationMethod.FILENAME);
}
```

- 注意指定`setSpecificationMethod(ObjectLocationSpecificationMethod.FILENAME);`这里设置的`ObjectLocationSpecificationMethod.FILENAME`值会在`ActiveMQConsumerDialog.getData()`用到



### 接口方法

```JAVA
@Override
public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
  return new ActiveMQConsumer(stepMeta, stepDataInterface, copyNr, transMeta, trans);
}

@Override
public StepDataInterface getStepData() {
  return new ActiveMQConsumerData();
}
```

这两个方法是接口必须实现的，按照模板来就行



### 成员变量

看代码注释

```java
//固定用法，配合BaseMessages类从配置文件中读取配置
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

```



`brokerUrl  queue config  msgIdField config` 等变量是核心，它们流转于Dialog、ActiveMQConsumer(StepInterface)中。

`injectedConfigNames、injectedConfigValues` 是用于辅助生成`config`变量的(可以丢掉)

`config`变量对应的是`Options Tab`中的属性，是可变化的(可删除、增加)

`msgField`封装成`ActiveMQConsumerField` 枚举类,是便于可扩展以及可流转。（后面再详细叙说）



### 其他方法

```java
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

public List<ActiveMQConsumerField> getFieldDefinitions() {
  return Lists.newArrayList(getMsgIdField(), getMsgField(), getTimestampField());
}

protected void setField(ActiveMQConsumerField field) {
  field.getInputName().setFieldOnMeta(this, field);
}

```

- `getRowMeta` 是用于获取输出的字段的，即一行数据由哪几列组成。在步骤初始化(`ActiveMQConsumer#init`)的时候被调用。
- `putFieldOnRowMeta` 组装一列数据（数据名称、类型）
- `getFieldDefinitions` 获取输出字段列表（只是简单的将成员变量组成列表）
- `setField(ActiveMQConsumerField field)` 这里比较绕--稍候描述



## ActiveMQConsumerDialog

`ActiveMQConsumerDialog` 继承了 `BaseStreamingDialog`，`BaseStreamingDialog`中实现了`open` 方法 ，所以不需要复写`open`方法，只需重写以下几个方法即可。

- getDialogTitle()-设置标题
- buildSetup(Composite wSetupComp) - 实现启动页面（必要的信息-服务器地址、队列名称）
- getData()-重写此方法，将**meta**中的信息设置到**启动页面的元素和父类的Text** 或者 **其他Tab也中(如果有的话)**
- createAdditionalTabs() 在此方法里创建额外的Tab
- additionalOks(BaseStreamStepMeta meta)：确认按钮，将Dialog中的数据保存至meta中。保存**启动页、额外Tab页**数据
- getFieldNames() -如果创建了Field Tab,这里对应的是Output Name(第2列)
- getFieldTypes() -如果创建了Field Tab,这里对应的是Type(第3列)



### 构造方法

```java
public ActiveMQConsumerDialog(Shell parent, Object in, TransMeta tr, String sname) {
  super(parent, in, tr, sname);
  this.consumerMeta = (ActiveMQConsumerMeta) in;
}
```

需要注意的是 第二个参数是**Object**(实际是`ActiveMQConsumerMeta`对象)



### getData()

```java
@Override
protected void getData() {
  ...
  switch ( specificationMethod ) {
    case FILENAME:
      wTransPath.setText(Const.NVL(meta.getFileName(), ""));
      break;
    case REPOSITORY_BY_NAME:
      String fullPath = Const.NVL(meta.getDirectoryPath(), "") + "/" + Const.NVL(meta.getTransName(), "");
      wTransPath.setText(fullPath);
      break;
    case REPOSITORY_BY_REFERENCE:
      referenceObjectId = meta.getTransObjectId();
      getByReferenceData(referenceObjectId);
      break;
    default:
      break;
  }
  ...
}
```

这一段直接抄过来即可。



### additionalOks()

将Dialog中的数据保存至meta中。保存**启动页、额外Tab页**数据

```java
@Override
protected void additionalOks(BaseStreamStepMeta meta) {
  consumerMeta.setBrokerUrl(wBrokerUrl.getText());
  consumerMeta.setQueue(wQueue.getText());
  //将field值设置到meta中
  setFieldsFromTable();
  //将option中的值设置到meta中
  setOptionsFromTable();
}
```

注意一下setFieldsFromTable()方法=>保存field

```java
/**
 * 将field值设置到meta中
 */
private void setFieldsFromTable() {
  int itemCount = fieldsTable.getItemCount();
  for (int rowIndex = 0; rowIndex < itemCount; rowIndex++) {
    TableItem row = fieldsTable.getTable().getItem(rowIndex);
    String inputName = row.getText(1);
    String outputName = row.getText(2);
    String outputType = row.getText(3);

    final ActiveMQConsumerField.Name ref = ActiveMQConsumerField.Name.valueOf(inputName.toUpperCase());

    final ActiveMQConsumerField field = new ActiveMQConsumerField(ref, outputName,
                                                                ActiveMQConsumerField.Type.valueOf(outputType));
    consumerMeta.setField(field);
  }
}
```

将`Field Table`中每一行数据 实例化成 `ActiveMQConsumerField`对象，然后`set`到`meta`中。

`consumerMeta.setField(field);`最终会调用 类似 `consumerMeta.setMsgField` 等具体的set方法,可以仔细研究一下`ActiveMQConsumerField`类



### getFieldNames()

getFieldNames()和getFieldTypes() 从描述来看，其实是提取Field Tab中的值，但它们的实际作用是什么呢？

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702143547.png)

如上图所示，当点击了New(新建转换)并保存后，在新文件中的Get records from stream步骤中就会有Field Tab中的值了

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702143708.png)

## ActiveMQConsumerData

`ActiveMQConsumerData` 继承自 `TransExecutorData` ，只有一个成员变量 `RowMetaInterface outputRowMeta`=>存储[行元数据]



## ActiveMQConsumer

`ActiveMQConsumer`继承自`BaseStreamStep`，所以无需重写`processRow()`,只需重写`init()`方法即可。



```java
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
```

以上是init方法的全部内容。我们来分段看。



```java
try {
  //创建[行元数据]-即:输出哪些字段
  data.outputRowMeta = meta.getRowMeta(getStepname(), this);
} catch (KettleStepException e) {
  log.logError(e.getMessage(), e);
}
```

`meta.getRowMeta(getStepname(), this);`刚刚在`ActiveMQConsumerMeta`中已经介绍过了。主要是构建[行数据]-即列名称、类型。



`connection = ActiveMQFactory.getConn(meta.getActiveMQEntity());`从meta中获取服务器地址、队列名称等信息来获取连接。



```java
//subtransExecutor:子转换执行器
window = new FixedTimeStreamWindow<>(
  subtransExecutor,
  data.outputRowMeta,
  getDuration(),
  getBatchSize());
```

固定这样写，将 `data.outputRowMeta`【行元数据】传给子窗口即可



`source = new ActiveMQStreamSource(connection, meta, data, this);`

`source`是父类`BaseStreamStep`的一个成员变量`protected StreamSource<List<Object>> source` ，所以我们的`ActiveMQStreamSource`是`StreamSource<List<Object>>`的实现类。

主要的职责是消费ActiveMQ的数据，然后传递给子窗口，怎么传递不需要关心。

我们现在看`ActiveMQStreamSource`代码。



## ActiveMQStreamSource

在open()方法中有这样一段代码：

```java
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
```

目的是找出某一列的位置。 假如：Message-1  MessageId-2



> ```
> callable = new ActiveMQConsumerCallable(connection, super::close);
> future = executorService.submit(callable);
> ```

具体的消费线程`ActiveMQConsumerCallable`



```java
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
```

一直尝试拉取activemq的数据，如果有数据，调用`processMessageAsRow(msg)`处理数据，然后调用`acceptRows(rows)`传递给后续的步骤处理。



```java
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
```

processMessageAsRows其实就是将从active mq拿到的数据塞到对应的列(这也是为什么一开始要有positions = new HashMap<>(valueMetas.size())的原因)上去。



至此，ActiveMQ Consumer插件开发的主要步骤就介绍完毕了。