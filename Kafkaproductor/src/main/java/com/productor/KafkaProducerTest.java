package com.productor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.Properties;
/**
 * kafka 生产者
 */
public class KafkaProducerTest implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    public KafkaProducerTest(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.92.101:9092,192.168.92.102:9092,192.168.92.103:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("auto.commit.interval.ms", "5000");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String,String>(props);
        this.topic = "test";
    }

    public void run() {
        // 监听的文件路径
        String filePath = "D:/桌面文件/2024-4-7-0-9-3-19831793549500-Sina Visitor System-采集的数据-后羿采集器.txt";
        // 循环监听文件变化
        while (true) {
            try {
                // 读取文件内容并发送到 Kafka
                sendFileContentToKafka(filePath);
                // 每次检测文件间隔一段时间
                Thread.sleep(10000); // 10秒
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void sendFileContentToKafka(String filePath){
        //读取文件数据
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(
                    new FileReader("D:/桌面文件/2024-4-7-0-9-3-19831793549500-Sina Visitor System-采集的数据-后羿采集器.txt")
            );
            //初始化
            String line;
            while ((line = reader.readLine()) != null) {
                //发送给Kafka Broker的key/value 值对
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,"Node", line);
                //发送数据
                producer.send(producerRecord);
                System.out.println(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //启动kafka生产者主函数
    public static void main(String args[]) {
        KafkaProducerTest test = new KafkaProducerTest("KAFKA_TEST");
        Thread thread = new Thread(test);
        thread.start();
    }

}