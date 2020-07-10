package flink.socket;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

/**
 * @author guozhenjiang
 * @description
 * @create 2020-06-26 20:27
 */
public class SocketWindowWordCount {

	public static void main(String[] args) throws Exception {
		final int port;
		String host;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			port = params.getInt("port");
			host = params.getRequired("host");
		} catch (Exception e) {
			System.out.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
			return;
		}
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//source
		DataStream<String> text = env.socketTextStream(host, port, "\n");

		//解析数据、对数据进行分组、窗口函数和统计个数
		DataStream<WordCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordCount>() {
			private static final long serialVersionUID = 6800597108091365154L;

			@Override
			public void flatMap(String value, Collector<WordCount> out) throws Exception {
				for (String word : value.split("//s")) {
					out.collect(new WordCount(word, 1));
				}
			}
		}).keyBy("word")
				.timeWindow(Time.seconds(5),Time.seconds(2))
				.reduce((ReduceFunction<WordCount>) (value1, value2) -> new WordCount(value1.word, value1.count + value2.count));


		//sink
		windowCounts.print();



		env.execute("Socket Window WordCount");

	}
}