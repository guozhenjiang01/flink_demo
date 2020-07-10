package action;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author guozhenjiang
 * @description
 * @create 2020-06-15 16:55
 */
public class ActionCal {
	public static void main(String[] args) throws Exception {
		//获取执行环节
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		env.getCheckpointConfig().setCheckpointTimeout(60000);
		Properties properties = new Properties();
		properties.setProperty("auto.offset.reset", "latest");
		properties.setProperty("bootstrap.servers", "");
		properties.setProperty("group.id", "gzj_test");
		FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer("", new SimpleStringSchema(), properties);
		flinkKafkaConsumer.setStartFromLatest();



		DataStreamSource<String> stringDataStreamSource = env.addSource(flinkKafkaConsumer);




		SingleOutputStreamOperator<BgActionBean> bgActionBeanSingleOutputStreamOperator = stringDataStreamSource.map((MapFunction<String, BgActionBean>) s -> {
			final JSONObject jsonObject = JSONObject.parseObject(s);
			return BgActionBean.builder()
					.action(jsonObject.getString("action"))
					.actionp(jsonObject.getString("actipnp"))
					.actionp2(jsonObject.getString("actionp2"))
					.timeStamp(Long.parseLong(jsonObject.getString("__ts")))
					.build();
		}).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<BgActionBean>(Time.seconds(5)) {
			@Override
			public long extractTimestamp(BgActionBean bgActionBean) {
				return bgActionBean.getTimeStamp();
			}
		});
		final SingleOutputStreamOperator<String> process = bgActionBeanSingleOutputStreamOperator
				.keyBy("action")
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.process(new ProcessWindowFunction<BgActionBean, String, Tuple, TimeWindow>() {
					@Override
					public void process(Tuple tuple, Context context, Iterable<BgActionBean> iterable, Collector<String> collector) throws Exception {
						int count = 0;
						for (BgActionBean bgActionBean : iterable) {
							count++;
						}
						JSONObject result = new JSONObject();
						result.put("action", tuple.getField(0));
						result.put("count", count);
						collector.collect(result.toJSONString());
					}
				});
		process.print().setParallelism(1);



		env.execute(" Window Action Count");

	}

}
