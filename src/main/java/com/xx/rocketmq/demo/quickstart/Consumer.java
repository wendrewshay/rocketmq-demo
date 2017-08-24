package com.xx.rocketmq.demo.quickstart;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class Consumer {

    public static void main(String[] args) throws MQClientException {
        //指定组名初始化消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");

        //指定命名服务器地址
        //或者你可以通过设置环境变量NAMESRV_ADDR来指定命名服务器的地址，如代码：consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        //以防是一个新的消费者组，指定从哪开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //订阅一个或者多个消费主题
        consumer.subscribe("TopicTest", "*");

        //注册一个回调，该回调会在取自broker的消息到达的时候执行
        consumer.registerMessageListener((MessageListenerConcurrently)(msgs, consumeConcurrentlyContext) -> {
                System.out.printf(Thread.currentThread().getName() + " Receive New Message: " + msgs + "%n");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        //执行消费者实例
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
