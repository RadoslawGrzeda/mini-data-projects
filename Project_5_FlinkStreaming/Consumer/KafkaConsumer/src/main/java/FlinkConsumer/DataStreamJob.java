package FlinkConsumer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import Deserializer.JSONDeserializer;
import Dto.person;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<person> source = KafkaSource.<person>builder()
						.setBootstrapServers("localhost:9092")
								.setTopics("personFlink")
										.setGroupId("flinkPerson")
												.setStartingOffsets(OffsetsInitializer.earliest())
														.setValueOnlyDeserializer(new JSONDeserializer())
																.build();

		DataStream<person> personStream= env.fromSource(source,WatermarkStrategy.noWatermarks(),"KafkaSource");
		personStream.print();
		env.execute("Flink Java API Skeleton");
	}
}
