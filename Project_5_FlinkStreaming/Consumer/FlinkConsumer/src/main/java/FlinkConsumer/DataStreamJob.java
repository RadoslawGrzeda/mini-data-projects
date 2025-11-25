package FlinkConsumer;

import Deserializer.JSONDeserializer;
import Dto.person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;


import java.sql.Date;
import java.sql.PreparedStatement;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

	    String topic="person";
		
		String bootstrapServer="localhost:9092";
		String groupId="group1";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<person> kafkaSource = KafkaSource.<person>builder()
				.setBootstrapServers(bootstrapServer)
				.setTopics(topic)
				.setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new JSONDeserializer())
				.build();

		JdbcExecutionOptions execOption= new JdbcExecutionOptions.Builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();
		JdbcConnectionOptions connOption=new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl("jdbc:postgresql://172.23.48.1:5432/postgres")
				.withDriverName("org.postgresql.Driver")
				.withUsername("postgres")
				.withPassword("qwerty1234")
				.build();

		DataStream<person> personStream=env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"KafkaSource");

//		personStream.print();
//		env.execute("KafkaSource");
		personStream.addSink(
				JdbcSink.sink(
						"INSERT INTO FromFlink (" +
								"firstName, lastName, address, email, phoneNumber, dateOfBirth, sex" +
								") VALUES (?, ?, ?, ?, ?, ?, ?) ",
						(PreparedStatement statement, person p) -> {
							statement.setString(1, p.getFirstName());
							statement.setString(2, p.getLastName());
							statement.setString(3, p.getAddress());
							statement.setString(4, p.getEmail());
							statement.setString(5, p.getPhoneNumber());
							statement.setDate(6, Date.valueOf(p.getDateOfBirth()));
							statement.setString(7, p.getSex());
						},
						execOption,
						connOption
				)
		).name("insert Into postgres");
		env.execute("KafkaSource");
	}
}