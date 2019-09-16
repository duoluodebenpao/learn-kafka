package com.frank.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者发送数据的流程
 * 1. 构造配置类，填充配置参数
 * 2. 通过配置类构造Producer生产者对象
 * 3. 构造装载数据的数据对象
 * 4. 将数据对象调用send方法发送到内存中
 * <p>
 * 扩展：
 * 1. 实现分区器
 * 2. 实现回调函数
 * 3. 系统默认是异步发送，可以实现同步发送
 *
 * @author fzj
 * @date 2019/9/9 12:06
 */
public class KafkaProducerDemo<k, v> extends Thread {

    private Properties props;
    private KafkaProducer<k, v> producer;
    private String topic = "test_producer";
    private Boolean isAsync = true;

    public Properties getProps() {
        if (props == null) {
            props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public KafkaProducer<k, v> getProducer() {
        if (producer == null) {
            if (props == null){
                getProps();
            }
            producer = new KafkaProducer<k, v>(props);
        }
        return producer;
    }

    public void setProducer(KafkaProducer<k, v> producer) {
        this.producer = producer;
    }

    public KafkaProducerDemo(String topic, Boolean isAsync) {
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public KafkaProducerDemo() {
    }

    public KafkaProducerDemo(String topic) {
        this.topic = topic;
    }


    @Override
    public void run() {
        //1. 构造配置参数类
        if (props == null) {
            getProps();
        }
        //2. 构造生产者对象
        if (producer == null) {
            getProducer();
        }
        //3. 构造装载数据的数据对象
        for (int i = 0; i < 10; i++) {
            String key = "key_" + i;
            String value = "value_" + i;
            ProducerRecord<k, v> record = new ProducerRecord(topic, key, value);
            if (this.isAsync) {
                //4. 发送数据，调用send方法把数据对象异步发送到内存中  默认异步发送
                producer.send(record);
            } else {
                //4. 发送数据，调用send方法把数据对象同步发送给broker
                try {
                    producer.send(record).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

        producer.close();
    }


    /*
     * @param args
     */
    public static void main(String[] args) {
        new KafkaProducerDemo().start();
    }
}
