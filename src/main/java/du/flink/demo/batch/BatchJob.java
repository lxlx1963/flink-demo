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

package du.flink.demo.batch;

import com.alibaba.fastjson.JSON;
import du.flink.demo.model.AdvertisementMonitorData;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ReduceOperator;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink demo application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the demo execution environment
//		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		String path = "G:\\xcm_monitor_data_2019-08-20.0.log";
		DataSource<String> stringDataSource = env.readTextFile(path);
		ReduceOperator<AdvertisementMonitorData> reduce = stringDataSource.map(string -> JSON.parseObject(string, AdvertisementMonitorData.class))
				.groupBy("deviceNumber", "advertisementName", "sex")
				.reduce((ReduceFunction<AdvertisementMonitorData>) (advertisementMonitorData, t1) -> {
					advertisementMonitorData.setDuration(advertisementMonitorData.getDuration() + t1.getDuration());
					advertisementMonitorData.setExposuresNumber(advertisementMonitorData.getExposuresNumber() + t1.getExposuresNumber());
					advertisementMonitorData.setTouchNumber(advertisementMonitorData.getTouchNumber() + t1.getTouchNumber());
					advertisementMonitorData.setWatchNumber(advertisementMonitorData.getWatchNumber() + t1.getWatchNumber());
					return advertisementMonitorData;
				});
		reduce.writeAsText("G:\\monitor");
		env.execute("Flink Batch Java API Skeleton");
	}
}
