package com.processor

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 定义kafka的消费者用于消费数据
 */
object KafkaProcessor {
    def main(args: Array[String]): Unit = {
        // 创建 KafkaProcessor 实例
        val kafkaProcessor = new KafkaProcessor()
        // 启动
        kafkaProcessor.start()
    }
}

class KafkaProcessor {
    val brokers = "192.168.92.101:9092,192.168.92.102:9092,192.168.92.103:9092"
    val topics = "test"

    val sparkConf = new SparkConf().setAppName("KafkaStreamingApp").setMaster("local[2]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spark_KafKa",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topics), kafkaParams)
    )
    // 定义一个方法来启动处理逻辑
    def start(): Unit = {
        // 打印接收到的原始数据
        stream.map(_.value()).print()

        // 对接收的数据进行去重处理
        val deduplicatedStream = stream.transform { rdd =>
            // 根据标题进行去重，仅保留每个标题第一次出现的记录
            rdd.flatMap(record => record.value().split(",").headOption.map((_, record)))
                    .reduceByKey((record1, record2) => record1) // 根据标题进行去重
                    .values // 仅保留去重后的记录
        }
        // 打印去重后的数据
        deduplicatedStream.print()
        deduplicatedStream.foreachRDD { rdd =>
            rdd.foreachPartition { partitionOfRecords =>
                // 建立数据库连接
                val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/data?serverTimezone=UTC", "root", "123456")
                val statement: PreparedStatement = connection.prepareStatement("""
                                                                                 |INSERT INTO your_table (column1, column2, column3, column4)
                                                                                 |VALUES (?, ?, ?, ?)
                                                                                 |ON DUPLICATE KEY UPDATE
                                                                                 |  column1 = VALUES(column1),
                                                                                 |  column2 = VALUES(column2),
                                                                                 |  column3 = VALUES(column3);
        """.stripMargin)
                partitionOfRecords.foreach { record =>
                    // 对每个记录执行插入操作
                    val data = record.value().split(",") // 以逗号分隔
                    if (data.length >= 4) { // 检查数组长度是否至少为4
                    statement.setString(1, data(0)) //
                    statement.setString(2, data(1)) //
                    statement.setString(3, data(2)) //
                    statement.setString(4, data(3)) //
                        val rowsAffected = statement.executeUpdate()
                        if (rowsAffected > 0) {
                            println("更新或插入成功: " + record.value())
                        } else {
                            println("更新或插入失败: " + record.value())
                        }
                }else{
                        println("字符串分割后长度不足: " + record.value())
                    }
                }
                // 关闭连接
                statement.close()
                connection.close()
            }
        }
        // 启动 Spark Streaming 上下文
        ssc.start()
        ssc.awaitTermination()
    }
}