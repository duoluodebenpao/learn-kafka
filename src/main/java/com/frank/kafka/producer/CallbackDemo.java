package com.frank.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 回调函数会在 producer 收到 ack 时调用，为异步调用。
 * 该方法有两个参数，分别是 RecordMetadata 和 Exception，
 * 如果 Exception 为 null，说明消息发送成功，
 * 如果 Exception 不为 null，说明消息发送失败
 *
 * @author fzj
 * @date 2019/9/11 22:24
 */
public class CallbackDemo implements Callback {

    private static long successTotal;
    private static long errTotal;

    /**
     * 实现计数功能的回调函数
     *
     * @param recordMetadata
     * @param e
     */
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        String message = recordMetadata.toString();
        System.out.println(message);
        if (e == null) {
            successTotal += 1;
            System.out.println(String.format("send success! successTotal [%d}]", successTotal));
        } else {
            errTotal += 1;
            System.out.println(String.format("send err! errTotal [%d]", errTotal));
        }
    }


    public static void main(String[] args) {
        String topic = "test_callback";
        KafkaProducerDemo<String, Long> myKafkaProducer = new KafkaProducerDemo();
        KafkaProducer<String, Long> producer = myKafkaProducer.getProducer();

        for (int i = 0; i < 10; i++) {
            String key = "key_" + i;
            Long value = Long.valueOf(i);
            ProducerRecord<String, Long> record = new ProducerRecord(topic, key, value);

            producer.send(record, new CallbackDemo());
        }

        producer.close();
    }
}
