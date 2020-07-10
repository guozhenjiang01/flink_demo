package flink.learn;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;


/**
 * @author guozhenjiang
 * @description
 * @create 2020-06-26 22:16
 */
public class SourceLearn {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//		final DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
//		final DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 0);
//		integerDataStreamSource.print();

		// redis
		final DataStreamSource dataStreamSource = executionEnvironment.addSource(new MySensorSource());
		FlinkJedisClusterConfig conf = new FlinkJedisClusterConfig.Builder().setNodes(Sets.newConcurrentHashSet()).build();

		dataStreamSource.addSink(new RedisSink(conf, new RedisMapper() {
			@Override
			public RedisCommandDescription getCommandDescription() {
				return null;
			}

			@Override
			public String getKeyFromData(Object o) {
				return null;
			}

			@Override
			public String getValueFromData(Object o) {
				return null;
			}
		}));
		//es
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "my-cluster-name");
		config.put("bulk.flush.max.actions", "1");
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));
		dataStreamSource.addSink(new ElasticsearchSink(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) {
				Map<String, String> json = new HashMap<>();
				json.put("data", element);

				return Requests.indexRequest()
						.index("my-index")
						.type("my-type")
						.source(json);
			}

			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				indexer.add(createIndexRequest(element));
			}
		}));

		executionEnvironment.execute("Socket Window WordCount");
	}

	static class MySensorSource implements SourceFunction<Double> {
		volatile boolean flag = true;

		@Override
		public void run(SourceContext ctx) throws Exception {
			if (flag) {
				Random random = new Random();
				IntStream.range(0, 100).forEach(one -> ctx.collect(one + random.nextGaussian()));
			}
		}

		@Override
		public void cancel() {
			flag = false;
		}
	}
}
