package du.flink.batch;

import com.alibaba.fastjson.JSON;
import du.flink.batch.model.FaceData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * 清洗人脸数据（年龄维度）
 *
 * @author dxy
 * @date 2019/11/4 10:04
 */
public class CountJob {
	public static void main(String[] args) throws Exception {
		// 执行环境
//		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		// hdfs路径
//		String hdfsPath = "hdfs://dev-base-bigdata-bj1-01:8020/flume/face/";
		String hdfsPath = "E:\\face";

		// 读取文件，获得DataSource
		DataSource<String> dataSource = env.readTextFile(hdfsPath);
		// 执行业务逻辑处理

		// 转化数据
		MapOperator<String, FaceData> faceDataMap = dataSource.map(faceDataStr -> JSON.parseObject(faceDataStr, FaceData.class));
		System.out.println("faceDataMap.collect().size(): " + faceDataMap.collect().size());

		// 以“年龄”分组，统计人次（实现count功能）

//		GroupReduceOperator<FaceData, Tuple2<String, Integer>> ageRange = faceDataMap.groupBy("ageRange").reduceGroup(new GroupReduceFunction<FaceData, Tuple2<String, Integer>>() {
//			@Override
//			public void reduce(Iterable<FaceData> in, Collector<Tuple2<String, Integer>> out) throws Exception {
//				String key = "";
//				int count = 0;
//				for (FaceData faceData : in) {
//					key = faceData.getAgeRange();
//					++count;
//				}
//
//				out.collect(new Tuple2<>(key, count));
//			}
//		});


		// 以“年龄、性别”分组，统计人次（实现count功能）

		GroupReduceOperator<FaceData, Tuple3<String, String, Integer>> reduce = faceDataMap.filter(faceData -> faceData != null)
				.filter(faceData -> StringUtils.isNotBlank(faceData.getGender()))
				.groupBy("ageRange", "gender")
				.reduceGroup(new RichGroupReduceFunction<FaceData, Tuple3<String, String, Integer>>() {
					@Override
					public void reduce(Iterable<FaceData> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
						String ageRangeKey = "";
						String genderKey = "";
						int count = 0;
						for (FaceData faceData : in) {
							ageRangeKey = faceData.getAgeRange();
							genderKey = faceData.getGender();
							++count;
						}
						out.collect(new Tuple3<>(ageRangeKey, genderKey, count));
					}
				});

		reduce.writeAsText(hdfsPath);
		// 以“年龄”分组，统计人数
//		UnsortedGrouping<FaceData> faceDataUnsortedGrouping = faceDataMap.groupBy("ageRange", "visitorId");


		env.execute("清洗人脸数据（年龄维度）");
	}

}
