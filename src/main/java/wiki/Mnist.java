package wiki;

import java.io.File;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;
import java.nio.file.Files;
import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import java.io.IOException;

public class Mnist {
    public static void main(String[] args) throws Exception {
//      String base = "/data/home/chikako_takasaki/work/tensorflow/";
      String base = "/Users/takasakichikako/my_research/work/tensorflow/";
      final File modelFile = new File(base + "mnist_dense.pb");
      byte graphDef[] = Files.readAllBytes(modelFile.toPath());
      Graph graph = new Graph();
      graph.importGraphDef(graphDef);
      Session session = new Session(graph);
      float[][] d = loadFeatures(base + "MNIST_data/t10k-images-idx3-ubyte.gz");
      for(int i=0; i<10000; i++){
        for(int j=0; j<784; j++){
          d[i][j] = d[i][j]/255.0f;
        }
      }
      System.out.println(d[0][202]);
      float[] l = loadLabels(base + "MNIST_data/t10k-labels-idx1-ubyte.gz");
      Tensor<Float> data = Tensor.create(d,Float.class);
      Tensor<Float> label = Tensor.create(l,Float.class);
      Tensor res = session.runner().feed("input", data).fetch("output/Softmax").run().get(0);

      float[][] res_float= new float[10000][10];
      res.copyTo(res_float);
      int n = 0;
      for(int i = 0; i < 10000; i++){
        float max = 0f;
        int maxIndex = 0;
        for(int j = 0; j < 10; j++){
//          System.out.print(res_float[i][j]);
          if(res_float[i][j] > max){
            max = res_float[i][j];
            maxIndex = j;
          }
        }
        System.out.print(maxIndex);
//        System.out.println();
        if((float)maxIndex == l[i]){
          n++;
        }
      }
      double acc = n/10000.0;
      System.out.println("結果：" + res_float.length);
      System.out.println("結果：" + res.toString());
      System.out.println("結果：" + acc);
      data.close();
      label.close();
      res.close();
      session.close();
      graph.close();
    }

    private static float[][] loadFeatures(String fileName) throws IOException {
      System.out.println("Loading feature data from " + fileName + " ...");
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
}
