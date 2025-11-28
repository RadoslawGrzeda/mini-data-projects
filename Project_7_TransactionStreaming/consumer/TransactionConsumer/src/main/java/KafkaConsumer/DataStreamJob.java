package KafkaConsumer;

import Deserializer.myJsonDeserializier;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;


public class DataStreamJob {




	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String topicName = "transactions";
		String groupId = "transactions_group";
		String BootstrapServer = "localhost:9092";

		KafkaSource<Transaction> kfSource = KafkaSource.<Transaction>builder()
				.setBootstrapServers(BootstrapServer)
				.setTopics(topicName)
				.setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new myJsonDeserializier())
				.build();

		DataStream<Transaction> transactionStream = env.fromSource(kfSource, WatermarkStrategy.noWatermarks(), "transactions");

		transactionStream.print();
		env.execute("Transaction Flink");
	}}
//
//		SimpleStringSchema())ecute("Transaction Flink");
//		KafkaSource<String> kfSource = KafkaSource.<String>builder()
//				.setBootstrapServers(BootstrapServer)
//				.setTopics(topicName)
//				.setGroupId(groupId)
//				.setStartingOffsets(OffsetsInitializer.latest())
//				.setValueOnlyDeserializer(new SimpleStringSchema())
//				.build();
//
//		DataStream<String> transactionStream=env.fromSource(kfSource, WatermarkStrategy.noWatermarks(),"transactions");
//
//		transactionStream.print();
//
//
//	}
