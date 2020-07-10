package flink.socket;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author guozhenjiang
 * @description
 * @create 2020-07-10 09:20
 */
public class ElasticSearchSinkUtil {
	public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
	                               SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func) {
		ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
		esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
		data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
	}

	/**
	 * 解析配置文件的 es hosts
	 *
	 * @param hosts
	 * @return
	 * @throws MalformedURLException
	 */
	public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
		String[] hostList = hosts.split(",");
		List<HttpHost> addresses = new ArrayList<>();
		for (String host : hostList) {
			if (host.startsWith("http")) {
				URL url = new URL(host);
				addresses.add(new HttpHost(url.getHost(), url.getPort()));
			} else {
				String[] parts = host.split(":", 2);
				if (parts.length > 1) {
					addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
				} else {
					throw new MalformedURLException("invalid elasticsearch hosts format");
				}
			}
		}
		return addresses;
	}
//	配置文件中读取 es 的地址
//		List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses("");
//		//从配置文件中读取 bulk flush size，代表一次批处理的数量，这个可是性能调优参数，特别提醒
//		int bulkSize = 1;
//		//从配置文件中读取并行 sink 数，这个也是性能调优参数，特别提醒，这样才能够更快的消费，防止 kafka 数据堆积
//		int sinkParallelism = 1;
//		//自己再自带的 es sink 上一层封装了下
//		ElasticSearchSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, windowCounts,
//				(WordCount wordCount, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
//					final IndexRequest gzj = Requests.indexRequest()
//							.index("gzj" + "_20200710")  //es 索引名
//							.type("gzj") //es type
//							.source(JSON(wordCount), XContentType.JSON);
//					requestIndexer.add(gzj);
//				});
}
