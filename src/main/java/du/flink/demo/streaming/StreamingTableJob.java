package du.flink.demo.streaming;

import du.flink.demo.model.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @author dxy
 * @date 2019/11/12 16:32
 */
public class StreamingTableJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Order> orderA = env.fromCollection(Arrays.asList(
				new Order(1L, "beer", 3),
				new Order(1L, "diaper", 4),
				new Order(3L, "rubber", 2)));
		orderA.timeWindowAll(Time.minutes(5L));

		DataStream<Order> orderB = env.fromCollection(Arrays.asList(
				new Order(2L, "pen", 3),
				new Order(2L, "rubber", 3),
				new Order(4L, "beer", 1)));
		orderB.timeWindowAll(Time.minutes(5L));

		System.out.println("-------------运行程序----------------");

		// convert DataStream to Table
		Table tableA = tEnv.fromDataStream(orderA, "user, product, amount");
		// register DataStream as Table
		tEnv.registerDataStream("OrderB", orderB, "user, product, amount");

		// union the two tables
		Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
				"SELECT * FROM OrderB WHERE amount < 2");

		// hdfs路径
		tEnv.toAppendStream(result, Order.class).print();

		env.execute();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

}
