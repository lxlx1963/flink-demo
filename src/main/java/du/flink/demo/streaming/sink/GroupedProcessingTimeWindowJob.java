package du.flink.demo.streaming.sink;

import du.flink.demo.constant.DateConstant;
import du.flink.demo.util.DateUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author dxy
 * @date 2019/11/13 18:13
 */
public class GroupedProcessingTimeWindowJob {
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataStream<Tuple2<Long, Long>> stream = env.addSource(new DataSource());

		SingleOutputStreamOperator<Tuple2<Long, Long>> reduce = stream.keyBy(0)
				.timeWindow(Time.of(2500, MILLISECONDS), Time.of(500, MILLISECONDS))
				.reduce(new SummingReducer());


		// 添加 Elasticsearch Sink
		List<HttpHost> esHttphost = new ArrayList<>();
		esHttphost.add(new HttpHost("10.10.0.167", 9200, "http"));

		String day = DateUtils.getPastDay(0, DateConstant.DATE_YEAR_MONTH_DAY);

		ElasticsearchSink.Builder<Tuple2<Long, Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
				esHttphost,
				new ElasticsearchSinkFunction<Tuple2<Long, Long>>() {

					@Override
					public void process(Tuple2<Long, Long> faceData, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
						requestIndexer.add(createIndexRequest(faceData));
					}

					public IndexRequest createIndexRequest(Tuple2<Long, Long> faceData) {
						Map<String, String> json = new HashMap<>(16);
						json.put("key", faceData.f0.toString());
						json.put("value", faceData.f1.toString());
						return Requests.indexRequest()
								.index("hdfs-flink-sink-test-" + day)
								.type("face-data")
								.source(json);
					}
				}
		);

		esSinkBuilder.setBulkFlushMaxActions(1);
		esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

		reduce.addSink(esSinkBuilder.build());

		env.execute();
	}

	private static class FirstFieldKeyExtractor<Type extends Tuple, Key> implements KeySelector<Type, Key> {

		@Override
		@SuppressWarnings("unchecked")
		public Key getKey(Type value) {
			return (Key) value.getField(0);
		}
	}

	private static class SummingWindowFunction implements WindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, Window> {

		@Override
		public void apply(Long key, Window window, Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) {
			long sum = 0L;
			for (Tuple2<Long, Long> value : values) {
				sum += value.f1;
			}

			out.collect(new Tuple2<>(key, sum));
		}
	}

	private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
			return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
		}
	}

	/**
	 * Parallel data source that serves a list of key-value pairs.
	 */
	private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

			final long startTime = System.currentTimeMillis();

			final long numElements = 20000000;
			final long numKeys = 10000;
			long val = 1L;
			long count = 0L;

			while (running && count < numElements) {
				count++;
				ctx.collect(new Tuple2<>(val++, 1L));

				if (val > numKeys) {
					val = 1L;
				}
			}

			final long endTime = System.currentTimeMillis();
			System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
