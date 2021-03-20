package wiki;

import java.io.File;
import java.io.FileReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.file.Files;
import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import java.io.IOException;

public class Stair {
    public static void main(String[] args) throws Exception {
      String base = "/home/chikako_takasaki/work/flink/";
      final File modelFile = new File(base + "stair_nn.pb");
      final String dataFile = base + "s10_2_without_answer.csv";
      byte graphDef[] = Files.readAllBytes(modelFile.toPath());
      Graph graph = new Graph();
      graph.importGraphDef(graphDef);
      Session session = new Session(graph);
      final int lineCount = 100000;
      Data[] dataArray = new Data[lineCount];
      try (BufferedReader in = new BufferedReader(new FileReader(new File(dataFile)))){
            String line;
            int n = 0;
            while((line = in.readLine()) != null) {
              Data d = loadData(line);
              dataArray[n] = d;
              n += 1;
            }
      } catch (IOException e){ 
            e.printStackTrace();
            System.exit(-1);
        
      }
      
      System.out.println("Data load completed!");
      for(int i=0; i<lineCount; i++){
        Tensor<Float> data = Tensor.create(dataArray[i].features,Float.class);
        Tensor res = session.runner().feed("input", data).fetch("output/Softmax").run().get(0);
      
        float[][] res_float= new float[1][100];
        res.copyTo(res_float);
        float max = 0f;
        int maxIndex = 0;
        for(int j = 0; j < 100; j++){
//        System.out.print(res_float[i][j]);
          if(res_float[0][j] > max){
            max = res_float[0][j];
            maxIndex = j;
          }
        }
        System.out.print(maxIndex);
//      System.out.println();
//      if((float)maxIndex == l[i]){
//          n++;
//        }
//      }
//      double acc = n/10000.0;
//      System.out.println("結果：" + res_float.length);
//      System.out.println("結果：" + res.toString());
//      System.out.println("結果：" + acc);
        data.close();
        res.close();
      }
      session.close();
      graph.close();
    }

    static class Data {
      float[][] features;
      float[] labels;
    }

    private static Data loadData(String line) throws IOException {
      Data data = new Data();
      String[] row = line.split(",");
      float[][] features = new float[1][500];
      for(int i=0; i < 500; i++){
        features[0][i] = Float.parseFloat(row[i]);
      }
      data.features = features;
      return data;
    }
}
