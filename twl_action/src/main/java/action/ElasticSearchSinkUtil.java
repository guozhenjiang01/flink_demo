package action;

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
}
