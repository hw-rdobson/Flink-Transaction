package com.cloudera.flink;

import com.cloudera.flink.RetailTransaction;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TransactionMonitorJob {

    public static final String EVENT_TIME_KEY = "event.time";
    public static final String ENABLE_SUMMARIES_KEY = "enable.summaries";
    private static final Logger LOG = LoggerFactory.getLogger(TransactionMonitorJob.class);
    public static String TRANSACTION_INPUT_TOPIC_KEY = "transaction.input.topic";
    public static String BALANCE_OUTPUT_TOPIC = "balance.output.topic";
    public static String ALERT_OUTPUT_TOPIC = "alert.output.topic";


    public static void main(String [] args) throws Exception {
        if (args.length!= 1) {
            //throw new RuntimeException("Path to the properties file is expected as the only argument.");
            args = new String[]{"/Users/rdobson/IntelliJ/flink-transactions/config/job.properties"};
            LOG.info("Created from default path url");
        }
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        LOG.info("Creating Env");
        new TransactionMonitorJob().createApplicationPipeline(params)
                .execute("Kafka Transaction Processor Job");


        LOG.info("exiting!!!");
    }

    private static DataStream<RetailTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
        // We read the ItemTransaction objects directly using the schema
        FlinkKafkaConsumer<RetailTransaction> transactionSource = new FlinkKafkaConsumer<RetailTransaction>(
                params.getRequired(TRANSACTION_INPUT_TOPIC_KEY),
                new RetailTransactionSchema(),
                Utils.readKafkaProperties(params, true));

        transactionSource.setCommitOffsetsOnCheckpoints(true);
        transactionSource.setStartFromEarliest();


        return env.addSource(transactionSource)
                .name("Kafka Transaction Source")
                .uid("Kafka Transaction Source");
    }
   public static void writeBalanceOutput(ParameterTool params, DataStream<CustomerBalance> balanceResultStream) {
        // Balance output is written back to kafka in a tab delimited format for readability

        FlinkKafkaProducer<String> balanceOutputSink = new FlinkKafkaProducer<String>(
                params.getRequired(BALANCE_OUTPUT_TOPIC),
                new SimpleStringSchema(),
                Utils.readKafkaProperties(params, false));

        balanceResultStream
                .map(CustomerBalance::toString)
                .addSink(balanceOutputSink)
                .name("Kafka  Balance Result Sink")
                .uid("Kafka Balance Result Sink");
    }

    public static void writeAlertOutput(ParameterTool params, DataStream<CustomerBalance> balanceResultStream) {
        // Balance Alert output is written back to kafka in a tab delimited format for readability

        FlinkKafkaProducer<String> balanceOutputSink = new FlinkKafkaProducer<String>(
                params.getRequired(ALERT_OUTPUT_TOPIC),
                new SimpleStringSchema(),
                Utils.readKafkaProperties(params, false));

        balanceResultStream
                .map(CustomerBalance::toString)
                .addSink(balanceOutputSink)
                .name("Kafka  Balance Alert Sink")
                .uid("Kafka Balance Alert Sink");
    }


    private StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // We set max parallelism to a number with a lot of divisors
        env.setMaxParallelism(360);

        // Configure checkpointing if interval is set
        long cpInterval = params.getLong("checkpoint.interval.millis", TimeUnit.MINUTES.toMillis(1));
        if (cpInterval > 0) {
            CheckpointConfig checkpointConf = env.getCheckpointConfig();
            checkpointConf.setCheckpointInterval(cpInterval);
            checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
            checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.getConfig().setUseSnapshotCompression(true);
        }

        if (params.getBoolean(EVENT_TIME_KEY, false)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }

        return env;
    }
    public final StreamExecutionEnvironment createApplicationPipeline(ParameterTool params) throws Exception {

        // Create and configure the StreamExecutionEnvironment
        StreamExecutionEnvironment env = createExecutionEnvironment(params);

        // Read transaction stream
        DataStream<RetailTransaction> transactionStream = readTransactionStream(params, env);

        // map the incoming transactions to the processing class
        DataStream <CustomerBalance> processedTransactions = transactionStream.keyBy(t -> t.id_customer).process(new ProcessTransaction()).name("calculate balance").uid("calculate balance");
       // test for negative balance
        DataStream<CustomerBalance> balanceAlertStream = processedTransactions.filter(b -> b.balance <=0);

        // Write to Kafka
        writeBalanceOutput(params,processedTransactions);
        writeAlertOutput(params,balanceAlertStream);

        return env;
    }


}
