# FlinkKafka_StairActions
### STAIRActionsをKafka+Flinkシステムで解析する

## プログラム [src/main/java/wiki/]
- Producer.java : Kafkaへのキーポイント転送
- Get.java : 解析なし
- KafkaStairNN.java : NN解析
- KafkaStairLSTM.java : LSTM解析

## 機械学習モデル
- stair_nn.pb
- stair_lstm_dropout0.pb

## データセットファイル
- s10_2_without_answer.csv : データサイズが大きいので192.168.111.20のNASにおいてあります.

## 実行方法
1. Mavenインストール
2. `maven clean package`でjarファイル生成
3. Kafkaディレクトリに移動してconsumerをコンソールで起動  
```./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-return --max-messages 10000 --property print.timestamp=true > ${出力ファイル}```
  - --topic : 解析結果を受け取るKafkaトピックの設定（Flinkジョブで指定したtopic + -return）
  - --max-messages : 受信する最大メッセージ数
  - --property print.timestamp=true : メッセージ受信のタイムアウトを表示
4. Flink Web UI or このディレクトリででジョブを開始  
```mvn exec:java -Dexec.mainClass=wiki.Get(実行したいプログラム名) -Dexec.args="--topic test --timeout 5 --outfile file"```
  - --topic : データを受け取るKafkaトピックの設定 
  - --timeout : BufferTimeoutの設定 (指定しない場合はdefault)
  - --outfile : 出力先（指定しない場合はKafka出力）
5. このディレクトリでProducerプログラムを実行し、Kafka Brokerにデータ転送  
```mvn exec:java -Dexec.mainClass=wiki.Producer -Dexec.args="test"```
  - -Dexec.args内でKafka topicを指定(Flinkジョブのtopicとそろえる)
