package wiki;
import java.util.Properties;
import java.io.*;
import java.util.Scanner;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
 
public class Producer {
 
  public static void main(String[] args) {
    String topic = args[0] == null ? "teset" : args[0];
    // 接続時の設定値を Properties インスタンスとして構築する
    Properties properties = new Properties();
    // 接続先 Kafka ノード
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("acks", "all");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    //  Kafkaに送信するキーポイントファイル置き場所
    String file_name = "/home/chikako_takasaki/work/flink/s10_2_without_answer.csv";
    
    // ファイルopen & ByteArray変換(Kafkaへの転送のため)
    byte[][] buf_list = new byte[10000][2000];
    try{
      File file = new File(file_name);
      BufferedReader br = new BufferedReader(new FileReader(file));
      for(int i=0; i < 10000; i++) {
        String[] row = br.readLine().split(",");
        ByteBuffer bb = ByteBuffer.allocate(2000);
        for(int j=0; j < 500; j++) {
          bb.putFloat(4*j, Float.parseFloat(row[j]));
        }
        byte[] arr = new byte[bb.remaining()];
        bb.get(arr);
        buf_list[i] = arr;
      }
      br.close();
    }catch(FileNotFoundException e){
      System.out.println(e);
    }catch(IOException e){
      System.out.println(e);
    }
    // Producer を構築する
    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
    try {
      // トピックを指定してメッセージを送信する
      System.out.println(new Timestamp(System.currentTimeMillis()));
      for (int i = 0; i < 10000; i++) {
        producer.send(new ProducerRecord<String, byte[]>(topic, String.valueOf(System.currentTimeMillis()), buf_list[i]));
      }
      System.out.println(new Timestamp(System.currentTimeMillis()));
    } finally {
      producer.close();
    }
  }
 
}

