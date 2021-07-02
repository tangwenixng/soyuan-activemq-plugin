# soyuan-activemq-plugin
Kettle ActiveMQ插件-支持kettle 8.1.0.0-365

开发笔记请参考
- Consumer Dev Note.md 
- Product Dev Note.md

# 编译安装

注释: pom->maven-assembly-plugin->outputDirectory

> mvn clean package

# 使用

将`target/soyuan-activemq-plugin-1.0-SNAPSHOT`目录拷贝到 `kettle/plugins` 目录下


# 调试插件

1、 修改kettle启动脚本：（Windows修改Spoon.bat）

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702162102.png)

找到`PENTAHO_DI_JAVA_OPTIONS`,修改为:
> PENTAHO_DI_JAVA_OPTIONS="-Xms1024m -Xmx2048m -XX:MaxPermSize=256m -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transpo    rt=dt_socket,server=y,suspend=n,address=1044"

即在原来的基础上增加了 `-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transpo    rt=dt_socket,server=y,suspend=n,address=1044`

2、 启动kettle

3、 配置IDEA

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702162448.png)

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702162544.png)

值如下：
> -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1044

4、启动idea

![](https://slimteaegg-blog.oss-cn-shanghai.aliyuncs.com/picgo/20210702162742.png)
