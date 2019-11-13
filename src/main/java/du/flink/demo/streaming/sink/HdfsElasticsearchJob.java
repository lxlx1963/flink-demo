package du.flink.demo.streaming.sink;

import du.flink.demo.model.Order;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * @author dxy
 * @date 2019/11/12 16:32
 */
public class HdfsElasticsearchJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 添加 Elasticsearch Sink
		List<HttpHost> esHttphost = new ArrayList<>();
		esHttphost.add(new HttpHost("10.10.0.167", 9200, "http"));

		ElasticsearchSink.Builder<Order> esSinkBuilder = new ElasticsearchSink.Builder<>(
				esHttphost,
				new ElasticsearchSinkFunction<Order>() {

					@Override
					public void process(Order order, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
						requestIndexer.add(createIndexRequest(order));
					}

					public IndexRequest createIndexRequest(Order order) {
						Map<String, String> json = new HashMap<>(16);
						json.put("user", order.user.toString());
						json.put("product", order.product);
						json.put("amount", String.valueOf(order.amount));
						return Requests.indexRequest()
								.index("flink-sink-test")
								.type("face-data")
								.source(json);
					}
				}
		);

		esSinkBuilder.setBulkFlushMaxActions(1);
		esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

		env.fromCollection(Arrays.asList(
				new Order(1L, "beer", 6),
				new Order(3L, "diaper", 4),
				new Order(4L, "pen", 8),
				new Order(2L, "rubber", 3),
				new Order(5L, "beer", 1),
				new Order(6L, "rubber", 2)))
				.keyBy("product")
				.window(TumblingProcessingTimeWindows.of(Time.minutes(5L)))
				.max("amount")
				.addSink(esSinkBuilder.build());

		env.execute("Flink-Sink-Test");
	}


}
