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

public class KafkaStairNN {
    public static void main(String[] args) throws Exception {
      final String output;
      final String topic;
      final String timeout;
      try {
        final ParameterTool params = ParameterTool.fromArgs(args);
        output = params.has("outfile") ? params.get("outfile") : null;
        topic = params.has("topic") ? params.get("topic") : "stair";
        timeout = params.has("timeout") ? params.get("timeout") : null;
      } catch (Exception e) {
        return;
      }

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      if(timeout == null){
        // checkpoint生成の時間間隔[ms]
        env.enableCheckpointing(5000);
      }else{
        env.enableCheckpointing(5000).setBufferTimeout(Integer.valueOf(timeout));
      }
      
      FlinkKafkaConsumer<Tuple4<Integer, Long, byte[], byte[]>> consumer = createKafkaConsumerMeta(topic);
      FlinkKafkaProducer<String> producer = createKafkaProducer(topic+"-return");
      
      // 機械学習モデルファイルの置き場所
      String base = "/opt/flink/";
      env.registerCachedFile("file://"+base+"stair_nn.pb", "graphFile", true);
      DataStream<Tuple4<Integer, Long, byte[], byte[]>> stream = env.addSource(consumer);

      if(output == null){
        stream.map(new ReflectorWithMeta("graphFile")).addSink(producer);
      }else{
        stream.map(new ReflectorWithMeta("graphFile")).writeAsText(output, WriteMode.OVERWRITE);
      }
      env.execute(); 
  
      Runtime.getRuntime().addShutdownHook(new Thread(
        () -> {
          System.out.println("Graph and Session closed.");
        }
      ));
    }

    // メタ情報なしver
    public static class Reflector extends RichMapFunction<byte[], String> {
      public String fileName;
      public Graph graph;
      public Session session;
      Reflector(String fileName){
        this.fileName = fileName;
      }

      @Override
      public void open(Configuration config) {
        try {
          File myFile = getRuntimeContext().getDistributedCache().getFile(fileName);
          this.graph = createModel(myFile);
          this.session = new Session(this.graph);
        } catch(IOException e) {
          System.err.println(e.getMessage());
        }
      }
      
      @Override
      public void close() {
        this.graph.close();
        this.session.close();
      }

      @Override
      public String map(byte[] stairBytes) throws Exception {
        float[][] stair = new float[1][stairBytes.length/4];
        ByteBuffer buf = ByteBuffer.wrap(stairBytes);
        for(int i=0; i<stairBytes.length/4; i++) {
          stair[0][i] = buf.getFloat(i*4);
        }
        Tensor input = Tensor.create(stair, Float.class);
        Tensor res = session.runner().feed("input", input).fetch("output/Softmax").run().get(0);
        
        float[][] res_float = new float[1][100];
        res.copyTo(res_float);
        
        String ans = "";
        float max = 0f;
        int maxIndex = 0;
        for (int i = 0; i < 100; i++){
          if(res_float[0][i] > max){
            max = res_float[0][i];
            maxIndex = i;
          }
        }
        ans = String.valueOf(maxIndex);
        res.close();
//        return String.valueOf(imageBytes[202]);
        
        return ans;
      }
    }

    // メタ情報ありver
    public static class ReflectorWithMeta extends RichMapFunction<Tuple4<Integer, Long, byte[], byte[]>, String> {
      public String fileName;
      public Graph graph;
      public Session session;
      ReflectorWithMeta(String fileName){
        this.fileName = fileName;
      }

      @Override
      public void open(Configuration config) {
        try {
          File myFile = getRuntimeContext().getDistributedCache().getFile(fileName);
          this.graph = createModel(myFile);
          this.session = new Session(this.graph);
        } catch(IOException e) {
          System.err.println(e.getMessage());
        }
      }
      
      @Override
      public void close() {
        this.graph.close();
        this.session.close();
      }

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
        
        Tensor input = Tensor.create(stair, Float.class);
        
        long tensor_created = System.nanoTime();

        Tensor res = session.runner().feed("input", input).fetch("output/Softmax").run().get(0);
        long inferred = System.nanoTime();

        float[][] res_float = new float[1][100];
        res.copyTo(res_float);
        
        String ans = "";
        float max = 0f;
        int maxIndex = 0;
        for (int i = 0; i < 100; i++){
          if(res_float[0][i] > max){
            max = res_float[0][i];
            maxIndex = i;
          }
        }
        ans = String.valueOf(maxIndex);
        res.close();

        long end = System.nanoTime();
        long end_millis = System.currentTimeMillis();
        
        String ret = sendTime + " : " + String.valueOf(start_millis) + " : " + String.valueOf(tensor_created - start) + " : " + String.valueOf(inferred - tensor_created) + " : " + String.valueOf(end - inferred) + " : " + String.valueOf(end_millis) + " : " + String.valueOf(partition) + " : " + String.valueOf(offset) + " : " + ans;
        return ret;
      }
    } 

    // 機械学習モデル初期化用
    private static class Model{
      Graph graph;
      Session session;
    }
 
    private static Graph createModel(File modelFile) throws IOException {
      byte graphDef[] = Files.readAllBytes(modelFile.toPath());
      Graph graph = new Graph();
      graph.importGraphDef(graphDef);
      return graph;    
    }

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
      props.setProperty("bootstrap.servers", "172.30.32.140:9092"); // Kafka Broker host:port
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
      System.out.println(topic);
      return consumer;
    }
}
