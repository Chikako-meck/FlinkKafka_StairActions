package wiki;

import java.util.Properties;
import java.io.File;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.tuple.Tuple4;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;
import java.nio.file.Files;
import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class Get {
    public static void main(String[] args) throws Exception {
      final String output;
      final String topic;
      final String timeout;
      try {
        final ParameterTool params = ParameterTool.fromArgs(args);
        output = params.has("outfile") ? params.get("outfile") : null;
        topic = params.has("topic") ? params.get("topic") : "stair";
        timeout = params.has("timeout") ? params.get("timeout") : null;
        System.out.println(topic);
      } catch (Exception e) {
        return;
      }

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      if(timeout == null){
//        Checkpoint 設定 : エラー発生時に自動で再接続する時間間隔[ms]
        env.enableCheckpointing(5000);
      }else{
        env.enableCheckpointing(5000).setBufferTimeout(Integer.valueOf(timeout));
        env.setBufferTimeout(Integer.valueOf(timeout));
      }
      
      FlinkKafkaConsumer<Tuple4<Integer, Long, byte[], byte[]>> consumer = createKafkaConsumerMeta(topic);
      FlinkKafkaProducer<String> producer = createKafkaProducer(topic+"-return");

      DataStream<Tuple4<Integer, Long, byte[], byte[]>> stream = env.addSource(consumer);

      if(output == null){
        stream.map(new ReflectorWithMeta()).addSink(producer);
      }else{
        stream.map(new ReflectorWithMeta()).writeAsText(output, WriteMode.OVERWRITE);
      }
      
      env.execute(); 
  
      Runtime.getRuntime().addShutdownHook(new Thread(
        () -> {
          System.out.println("Graph and Session closed.");
        }
      ));
    }

    
    // メタ情報(kafka partition, offset, timestamp...)なしver
    public static class Reflector extends RichMapFunction<byte[], String> {
      @Override
      public String map(byte[] stairBytes) throws Exception {
        float[][] stair = new float[1][stairBytes.length/4];
        ByteBuffer buf = ByteBuffer.wrap(stairBytes);
        for(int i=0; i<stairBytes.length/4; i++) {
          stair[0][i] = buf.getFloat(i*4);
        }
        
        String ans = String.valueOf(stair[0][0]);
        
        return ans;
      }
    } 
    
    // メタ情報(kafka partition, offset, timestamp...)ありver
    public static class ReflectorWithMeta extends RichMapFunction<Tuple4<Integer, Long, byte[], byte[]>, String> {
      @Override
      public String map(Tuple4<Integer, Long, byte[], byte[]> data) throws Exception {
        long start_millis = System.currentTimeMillis();
        long start = System.nanoTime();

        int partition = data.f0;
        long offset = data.f1;
        byte[] messageKey = data.f2;
        byte[] stairBytes = data.f3;
        String sendTime = new String(messageKey);
        float[][] stair = new float[1][stairBytes.length/4];
        ByteBuffer buf = ByteBuffer.wrap(stairBytes);
        for(int i=0; i<stairBytes.length/4; i++) {
          stair[0][i] = buf.getFloat(i*4);
        }
        
        long get_data = System.nanoTime();

        String ans = String.valueOf(stair[0][0]);
        
        long end_millis = System.currentTimeMillis();
        
        String ret = sendTime + " : " + String.valueOf(start_millis) + " : " + String.valueOf(get_data - start) + " : " + String.valueOf(end_millis) + " : " + String.valueOf(partition) + " : " + String.valueOf(offset) + " : " + ans;
        return ret;
      }
    } 

// 解析なしプログラムなので、機械学習モデルの設定不要
/*    private static class Model{
      Graph graph;
      Session session;
    }
    
    private static Graph createModel(File modelFile) throws IOException {
      byte graphDef[] = Files.readAllBytes(modelFile.toPath());
      Graph graph = new Graph();
      graph.importGraphDef(graphDef);
      return graph;    
    }
*/

    private static FlinkKafkaProducer<String> createKafkaProducer(String topic) throws IOException {
      Properties props = new Properties();
      props.setProperty("bootstrap.servers", "172.30.32.140:9092"); // Kafka Broker host:port

      FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
        topic,
        new SimpleStringSchema(),
        props);
      return producer;
    }
    
    public static class CustomKeyedDeserializationSchema implements KeyedDeserializationSchema<Tuple4<Integer, Long, byte[], byte[]>> { 
      @Override
      public boolean isEndOfStream(Tuple4<Integer, Long, byte[], byte[]> nextElement) {
        return false;
      }
      
      @Override
      public Tuple4<Integer, Long, byte[], byte[]> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return new Tuple4<>(partition, offset, messageKey, message);
      }

      @Override
      public TypeInformation<Tuple4<Integer, Long, byte[], byte[]>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
      }
    }

    private static FlinkKafkaConsumer<byte[]> createKafkaConsumer(String topic) throws IOException {
      Properties props = new Properties();
      props.setProperty("bootstrap.servers", "172.30.32.140:9092");   // Kafka Broker host:port
      props.setProperty("group.id", topic);                 // Consumer group ID
    
      FlinkKafkaConsumer<byte[]> consumer =
          new FlinkKafkaConsumer<>(
              topic,
              new AbstractDeserializationSchema<byte[]>() {
                  @Override
                  public byte[] deserialize(byte[] bytes) throws IOException {
                      return bytes;
                  }
              },
              props);
      System.out.println(topic);
      return consumer;
    }

    private static FlinkKafkaConsumer<Tuple4<Integer, Long, byte[], byte[]>> createKafkaConsumerMeta(String topic) throws IOException {
      Properties props = new Properties();
      props.setProperty("bootstrap.servers", "172.30.32.140:9092"); // Kafka Broker host:port
      props.setProperty("group.id", topic);                 // Consumer group ID
    
      FlinkKafkaConsumer<Tuple4<Integer, Long, byte[], byte[]>> consumer =
          new FlinkKafkaConsumer<>(
              topic,
              new CustomKeyedDeserializationSchema(),
              props);
      return consumer;
    }
}
