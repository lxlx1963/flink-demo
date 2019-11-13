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

package du.flink.demo.streaming.sink;

import com.alibaba.fastjson.JSON;
import du.flink.demo.constant.DateConstant;
import du.flink.demo.model.FaceData;
import du.flink.demo.util.DateUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsToElasticsearchJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// 文件路径
		String filePath = "hdfs://dev-base-bigdata-bj1-01:8020/flume/face/";

		// 获取DataStream
		DataStreamSource<String> dataStream = env.readTextFile(filePath);

		SingleOutputStreamOperator<FaceData> reduce = dataStream.map(faceStr -> JSON.parseObject(faceStr, FaceData.class))
				.filter(faceData -> faceData != null)
				.filter(faceData -> faceData.getId() != null)
				.keyBy("ageRange", "gender")
				.windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5L)))
				.reduce((ReduceFunction<FaceData>) (faceData, t1) -> {
					t1.setId(faceData.getId() + t1.getId());
					return t1;
				});


		// 添加 Elasticsearch Sink
		List<HttpHost> esHttphost = new ArrayList<>();
		esHttphost.add(new HttpHost("10.10.0.167", 9200, "http"));

		String day = DateUtils.getPastDay(0, DateConstant.DATE_SHORT_ISO);

		ElasticsearchSink.Builder<FaceData> esSinkBuilder = new ElasticsearchSink.Builder<>(
				esHttphost,
				new ElasticsearchSinkFunction<FaceData>() {

					@Override
					public void process(FaceData faceData, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
						requestIndexer.add(createIndexRequest(faceData));
					}

					public IndexRequest createIndexRequest(FaceData faceData) {
						Map<String, String> json = new HashMap<>(16);
						json.put("ageRange", faceData.getAgeRange());
						json.put("gender", faceData.getGender());
						json.put("id", String.valueOf(faceData.getId()));
						return Requests.indexRequest()
								.index("flink-sink-test-" + day)
								.type("face-data")
								.source(json);
					}
				}
		);

		esSinkBuilder.setBulkFlushMaxActions(1);
		esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());


		reduce.addSink(esSinkBuilder.build());

		env.execute("Flink Streaming Sink Test");
	}
}
