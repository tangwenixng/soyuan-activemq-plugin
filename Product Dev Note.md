# ActiveMQ Product组件开发笔记

## 概览

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702151759.png)

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702151811.png)



## 前提

该插件基于`kettle 8.1.0.0-365` 开发

如果是其他版本，不保证可用。(由于继承的`BaseStreamingDialog`等父类会随版本而变化)

> 本插件模仿官方Kafka插件源码编写：
>
> https://github.com/pentaho/big-data-plugin/tree/master/kettle-plugins/kafka



暂不支持topic，需要的可自行修改源码(工程量应该不大)。



## 必备模板

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702151919.png)

相对ActiveMQ Consumer插件,Product插件相对来说简单多了。



由于Product不需要阻塞，所以就当作普通插件来开发。集成官方推荐的父类即可。

- ActiveMQProduct extends BaseStep implements StepInterface
- ActiveMQProductData extends BaseStepData implements StepDataInterface
- ActiveMQProductDialog extends BaseStepDialog implements StepDialogInterface
- ActiveMQProductMeta extends BaseStepMeta implements StepMetaInterface



## ActiveMQProductMeta

关键属性的话就下面4个，是获取ActiveMQ 连接和消费数据必备的属性

```java
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
```

- msgField: 这个字段是从前一个步骤获取的。这个字段对应的值就是我们发送到AMQ的值，所以很重要。



然后就是模板方法，也是必备的：

- getXML()
- loadXML()
- saveRep()
- readRep()

## ActiveMQProductDialog

继承自普通的BaseStepDialog



注意构造方法：**将Object强转成BaseStepMeta 和 ActiveMQProductMeta**

```java
public ActiveMQProductDialog(Shell parent, Object in, TransMeta transMeta, String stepname) {
  super(parent, (BaseStepMeta) in, transMeta, stepname);
  this.meta = (ActiveMQProductMeta) in;
}
```



唯一需要实现的是open()方法。open()很多代码可以直接copy过来。

需要自己实现`Setup`和`Options`标签



**其中Setup中的Message是需要从前一个步骤获取的**，代码如下：

```java
wMsgField = new ComboVar(transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
props.setLook(wMsgField);
wMsgField.addModifyListener(lsMod);
FormData fdMsgField = new FormData();
fdMsgField.left = new FormAttachment(0, 0);
fdMsgField.top = new FormAttachment(wlMsgField, 5);
fdMsgField.right = new FormAttachment(0, INPUT_WIDTH);
wMsgField.setLayoutData(fdMsgField);
Listener lsMsgFocus = event -> {
  String current = wMsgField.getText();
  wMsgField.getCComboWidget().removeAll();
  wMsgField.setText(current);
  //重要的地方：从前个步骤获取字段
  try {
    RowMetaInterface rmi = transMeta.getPrevStepFields(stepname);
    //上一步骤的所有列-添加到下拉框中
    final List<ValueMetaInterface> ls = rmi.getValueMetaList();
    for (int i = 0; i < ls.size(); i++) {
      final ValueMetaBase vmb = (ValueMetaBase) ls.get(i);
      wMsgField.add(vmb.getName());
    }
  } catch (KettleStepException e) {
    e.printStackTrace();
  }
};
wMsgField.getCComboWidget().addListener(SWT.FocusIn, lsMsgFocus);
```



剩下的就没什么好说的了。



## ActiveMQProduct

继承自 `BaseStep`，所以需要实现 `init()` 和 `processRow()`



### init 

固定的格式啊

```java
@Override
public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
  super.init(smi, sdi);
  meta = (ActiveMQProductMeta) smi;
  data = (ActiveMQProductData) sdi;
  return true;
}
```



### processRow

```java
if (first) {
  //找出我们选择的Message列在上一步骤中排第几列，存储到ActiveMQProductData.msgFieldIndex
  data.msgFieldIndex = getInputRowMeta().indexOfValue(environmentSubstitute(meta.getMsgField()));
  try {
    //还要创建AMQ连接,因为连接只需在刚开始时创建就行了，不要重复创建
    data.conn = ActiveMQFactory.getConn(meta.getActiveMQEntity());
  } catch (JMSException e) {
    //如果创建失败，就直接退出
    log.logError(e.getMessage(), e);
    setOutputDone();
    return false;
  }
  first = false;
}
```



```java
//r表示上一步骤传递过来的数据，在初始化的时候我们已经知道要去哪一列拿目标数据了
//所以这里的content就是我们要发送的数据
String content = (String) r[data.msgFieldIndex];
TextMessage msg = session.createTextMessage(content);
producer.send(msg);
//记得提交给AMQ
session.commit();
//提交后记录+1
incrementLinesOutput();
//表示在此步骤后还可以接上另一个步骤(原封不动地把上一步骤的数据转发到下一步骤)
putRow(getInputRowMeta(), r);
```


