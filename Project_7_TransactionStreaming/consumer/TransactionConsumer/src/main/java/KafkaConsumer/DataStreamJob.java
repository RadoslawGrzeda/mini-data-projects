package KafkaConsumer;

import Deserializer.myJsonDeserializier;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;


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

		DataStream<Transaction> usdTransaction = transactionStream.filter(filter -> filter.getCurrency().equals("USD"));
		DataStream<Transaction> eurTransaction = transactionStream.filter(filter -> filter.getCurrency().equals("EUR"));
		DataStream<Transaction> gbpTransaction = transactionStream.filter(filter -> filter.getCurrency().equals("GBP"));
		DataStream<Transaction> jpyTransaction = transactionStream.filter(filter -> filter.getCurrency().equals("JPY"));
		DataStream<Transaction> audTransaction = transactionStream.filter(filter -> filter.getCurrency().equals("AUD"));

		usdTransaction.addSink(createJdbcSink("usdTransaction"));
		eurTransaction.addSink(createJdbcSink("eurTransaction"));
		gbpTransaction.addSink(createJdbcSink("gbpTransaction"));
		jpyTransaction.addSink(createJdbcSink("jpyTransaction"));
		audTransaction.addSink(createJdbcSink("audTransaction"));

		env.execute("Transaction Flink");

		}

private static SinkFunction<Transaction> createJdbcSink(String tableName) {
	String sql = "Insert into " + tableName + " values (?,?,?,?,?,?,?,?)";

	JdbcExecutionOptions execOption = JdbcExecutionOptions.builder().withBatchIntervalMs(200).withBatchSize(1000).withMaxRetries(5).build();

	JdbcConnectionOptions connOption = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
			.withUrl("jdbc:postgresql://172.23.48.1:5432/postgres")
			.withDriverName("org.postgresql.Driver")
			.withUsername("postgres")
			.withPassword("qwerty1234")
			.build();

	return JdbcSink.<Transaction>sink(
			sql,
			(statement, transaction) -> {
				statement.setString(1, transaction.getTransaction_id());
				statement.setString(2, transaction.getUser_id());
				statement.setDouble(3, transaction.getAmount());
				statement.setString(4, transaction.getCurrency());
				statement.setTimestamp(5, Timestamp.valueOf(transaction.getTimestamp()));
				statement.setString(6, transaction.getMerchant());
				statement.setString(7, transaction.getCategory());
				statement.setString(8, transaction.getStatus());
			},
			execOption,
			connOption
	);

}

}
