/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package du.flink.demo.streaming;

import com.alibaba.fastjson.JSON;
import du.flink.demo.model.FaceData;
import du.flink.demo.model.dto.FaceDataDTO;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 * <p>
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 * <p>
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 * <p>
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// 文件路径
		String filePath = "hdfs://dev-base-bigdata-bj1-01:8020/flume/face/";
		filePath = "E:\\face";
		DataStreamSource<String> dataStream = env.readTextFile(filePath);


		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env);


		SingleOutputStreamOperator<FaceData> faceDataStream = dataStream.map(faceStr -> JSON.parseObject(faceStr, FaceData.class));


		fsTableEnv.registerDataStream("face_data", faceDataStream);

		String sql = "SELECT ageRange,COUNT(id) as total FROM face_data GROUP BY ageRange";

		Table table = fsTableEnv.sqlQuery(sql);

		DataStream<Tuple2<Boolean, FaceDataDTO>> faceDataDTODataStream = fsTableEnv.toRetractStream(table, FaceDataDTO.class);
		faceDataDTODataStream.timeWindowAll(Time.minutes(5L));

		// 5分钟读取一次数据
		System.out.println("-------读取数据--------");

		faceDataDTODataStream.print("-------Flink streaming 读取数据--------");
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
