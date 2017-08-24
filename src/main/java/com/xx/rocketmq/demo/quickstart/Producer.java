package com.xx.rocketmq.demo.quickstart;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {

    public static void main(String[] args) throws Exception {
        //初始化生产者（自定义组的名字）
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        //指定命名服务器地址
        producer.setNamesrvAddr("127.0.0.1:9876");

        //启动实例
        producer.start();

        for (int i = 0; i < 100; i++) {
            //创建消息实例，指定主题、标签和消息体
            Message msg = new Message("TopicTest", "TagA", ("Hello RocketMQ" + i).getBytes("UTF-8"));

            //调用发送消息方法将消息传输到其中一个broker
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }

        //一旦生产者不再使用就关闭
        producer.shutdown();
    }
}
