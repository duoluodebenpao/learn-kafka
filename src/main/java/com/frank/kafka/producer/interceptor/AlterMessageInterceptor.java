package com.frank.kafka.producer.interceptor;

import com.frank.kafka.producer.KafkaProducerDemo;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * @author fzj
 * @date 2019/9/12 20:34
 */
public class AlterMessageInterceptor implements ProducerInterceptor<String, String> {


    /**
     * 修改每条发送数据的记录
     *
     * @param producerRecord
     * @return
     */
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String topic = producerRecord.topic();
        Integer partition = producerRecord.partition();
        Long timestamp = producerRecord.timestamp();
        String key = producerRecord.key();
        String value = producerRecord.value() + "_" + System.currentTimeMillis();

        return new ProducerRecord<String, String>(topic, partition, timestamp, key, value);
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }

    public static void main(String[] args) {
        KafkaProducerDemo<String, String> producer = new KafkaProducerDemo<String, String>();
        Properties props = producer.getProps();

        //构建拦截链
        ArrayList<String> interceptors = new ArrayList<String>();
        interceptors.add(AlterMessageInterceptor.class.getName());

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        producer.run();
    }
}
