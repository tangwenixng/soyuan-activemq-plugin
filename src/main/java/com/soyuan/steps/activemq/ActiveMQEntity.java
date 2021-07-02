package com.soyuan.steps.activemq;

/**
 * @author tangwx@soyuan.com.cn
 * @date 2021/7/1 上午11:11
 */
public class ActiveMQEntity {
    private String brokerUrl;
    private String queue;
    private String username;
    private String password;

    public ActiveMQEntity(String brokerUrl, String queue, String username, String password) {
        this.brokerUrl = brokerUrl;
        this.queue = queue;
        this.username = username;
        this.password = password;
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
