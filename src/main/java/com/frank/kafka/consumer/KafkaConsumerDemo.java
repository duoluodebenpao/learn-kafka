package com.frank.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Consumer 消费数据时的可靠性是很容易保证的，因为数据在 Kafka 中是持久化的，故
 * 不用担心数据丢失问题。
 * 由于 consumer 在消费过程中可能会出现断电宕机等故障，consumer 恢复后，需要从故
 * 障前的位置的继续消费，所以 consumer 需要实时记录自己消费到了哪个 offset，以便故障恢
 * 复后继续消费。
 * 所以 offset 的维护是 Consumer 消费数据是必须考虑的问题。
 *
 * 需要用到的类：
 * KafkaConsumer：需要创建一个消费者对象，用来消费数据
 * ConsumerConfig：获取所需的一系列配置参数
 * ConsuemrRecord：每条数据都要封装成一个 ConsumerRecord 对象
 * 为了使我们能够专注于自己的业务逻辑，Kafka 提供了自动提交 offset 的功能。
 * 自动提交 offset 的相关参数：
 * enable.auto.commit ：是否开启自动提交 offset 功能
 * auto.commit.interval.ms ：自动提交 offset 的时间间隔
 *
 * @author fzj
 * @date 2019/9/12 20:55
 */
public class KafkaConsumerDemo<k,v> extends Thread {

    private Properties props;
    private KafkaConsumer<k,v> consumer;

    

    @Override
    public void run() {
        super.run();
    }
}
