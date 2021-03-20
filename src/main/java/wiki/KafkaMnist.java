package wiki;

import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;

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

public class KafkaMnist {
    public static void main(String[] args) throws Exception {
      final String output;
      try {
        final ParameterTool params = ParameterTool.fromArgs(args);
        output = params.has("outfile") ? params.get("outfile") : "flink.out";
      } catch (Exception e) {
        return;
      }

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      FlinkKafkaConsumer<byte[]> consumer = createKafkaConsumer("mnist");
//      FlinkKafkaConsumer<String> consumer = createKafkaConsumerString("mnist");
      consumer.setStartFromEarliest();
      System.out.println("Consumer created.");
      String base = "/data/home/chikako_takasaki/work/tensorflow/";
      env.registerCachedFile("file://"+base+"mnist_dense.pb", "graphFile", true);
      DataStream<byte[]> stream = env.addSource(consumer);
//      DataStream<String> stream = env.addSource(consumer);

      stream.map(new Reflector()).writeAsText(output, WriteMode.OVERWRITE);
//      stream.writeAsText(output, WriteMode.OVERWRITE);
      env.execute(); 
  
      Runtime.getRuntime().addShutdownHook(new Thread(
        () -> {
          System.out.println("Graph and Session closed.");
        }
      ));
    }

    public static class Reflector extends RichMapFunction<byte[], String> {
      private Graph graph;
      @Override
      public void open(Configuration config) {
        try {
          File myFile = getRuntimeContext().getDistributedCache().getFile("graphFile");
          this.graph = createModel(myFile);
        } catch(IOException e) {
          System.err.println(e.getMessage());
        }
      }
      
      @Override
      public void close() {
        this.graph.close();
      }

      @Override
      public String map(byte[] imageBytes) throws Exception {
        Session session = new Session(this.graph);
        float[][] image = new float[1][imageBytes.length];
        for(int i=0; i<imageBytes.length; i++) {
          image[0][i] = Float.valueOf(Byte.toUnsignedInt(imageBytes[i])/225.0f);
        }
        Tensor input = Tensor.create(image, Float.class);
        Tensor res = session.runner().feed("input", input).fetch("output/Softmax").run().get(0);
        
        float[][] res_float = new float[1][10];
        res.copyTo(res_float);
        
        String ans = "";
        float max = 0f;
        int maxIndex = 0;
        for (int i = 0; i < 10; i++){
          if(res_float[0][i] > max){
            max = res_float[0][i];
            maxIndex = i;
          }
        }
        ans = String.valueOf(maxIndex);
        res.close();
//        return String.valueOf(imageBytes[202]);
        session.close();
        return ans;
      }
    } 
/*      
      float[][] d = loadFeatures(base + "MNIST_data/t10k-images-idx3-ubyte.gz");
      float[] l = loadLabels(base + "MNIST_data/t10k-labels-idx1-ubyte.gz");
//      Tensor<Float> data = Tensor.create(d,Float.class);
//      Tensor<Float> label = Tensor.create(l,Float.class);
      float [][] d_new = new float[1][d[0].length];
      d_new[0] = d[0];
      Tensor res = model.session.runner().feed("input", Tensor.create(d_new, Float.class)).fetch("output").run().get(0);
//      float[][] res_float= new float[10000][10];
      float[][] res_float = new float[1][10];
      res.copyTo(res_float);
//      int n = 0;
//      for(int i = 0; i < 10000; i++){
//        for(int j = 0; j < 10; j++){
//          if(res_float[i][j] == 1.0f && (float)j == l[i])
//            n++;
//        }
//      }
//      double acc = n/10000.0 * 100;
//      System.out.println("結果：" + res.toString());
//      System.out.println("結果：" + acc);
      for (int i = 0; i < 10; i++){
        if(res_float[0][i] == 1.0f && (float)i == l[0])
          System.out.println("true");
      }   
//      data.close();
//      label.close();
      res.close();
      model.session.close();
      model.graph.close();
    }*/

    private static float[][] loadFeatures(String fileName) throws IOException {
      DataInputStream is = new DataInputStream(new GZIPInputStream(new FileInputStream(fileName)));
      is.readInt(); // Magic Number
      int numImages = is.readInt(); // num of images
      int numDimensions = is.readInt() * is.readInt(); // hight * width

      float[][] features = new float[numImages][numDimensions];
      for (int i = 0; i < numImages; i++) {
        for (int j = 0; j < numDimensions; j++) {
          features[i][j] = (float) is.readUnsignedByte();
        }
      }

      return features;
    }

    private static float[] loadLabels(String fileName) throws IOException {
      System.out.println("Loading label data from " + fileName + " ...");
      DataInputStream is = new DataInputStream(new GZIPInputStream(new FileInputStream(fileName)));

      is.readInt(); // Magic Number
      int numLabels = is.readInt(); // num of images

      float[] labels = new float[numLabels];
      for (int i = 0; i < numLabels; i++) {
         int label = is.readUnsignedByte();
         labels[i] = label;
      }
      return labels;
    }

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

    private static FlinkKafkaConsumer<byte[]> createKafkaConsumer(String topic) throws IOException {
      Properties props = new Properties();
//      props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
      props.setProperty("bootstrap.servers", "hokudai-kafka-1:9092"); // Broker default host:port
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

    private static FlinkKafkaConsumer<String> createKafkaConsumerString(String topic) throws IOException {
      Properties props = new Properties();
//      props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
      props.setProperty("bootstrap.servers", "hokudai-kafka-1:9092"); // Broker default host:port
      props.setProperty("group.id", topic);                 // Consumer group ID
    
      FlinkKafkaConsumer<String> consumer =
          new FlinkKafkaConsumer<>(
              topic,
              new SimpleStringSchema(),
              props);
      System.out.println("Consumer generated");
      return consumer;
    }

}
