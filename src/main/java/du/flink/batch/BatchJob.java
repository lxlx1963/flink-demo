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

package du.flink.batch;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
//		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		long begin = System.currentTimeMillis();
		LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();
//		String path = "D:\\face_data_monitor.log";
		String path = "C:\\Users\\Administrator\\Desktop\\xcm_monitor_data_2019-08-14.0.log";
		DataSource<String> stringDataSource = env.readTextFile(path);
		ReduceOperator<AdvertisementMonitorData> reduce = stringDataSource.map(string -> JSON.parseObject(string, AdvertisementMonitorData.class))
				.filter((FilterFunction<AdvertisementMonitorData>) advertisementMonitorData -> advertisementMonitorData.getScreenType().equals(1))
//				.groupBy("deviceNumber", "advertisementName")
				.groupBy("deviceNumber")
				.reduce((ReduceFunction<AdvertisementMonitorData>) (advertisementMonitorData, t1) -> {
					advertisementMonitorData.setDuration(advertisementMonitorData.getDuration() + t1.getDuration());
					advertisementMonitorData.setExposuresNumber(advertisementMonitorData.getExposuresNumber() + t1.getExposuresNumber());
					return advertisementMonitorData;
				});
		long end = System.currentTimeMillis();
		System.out.println("end - begin: "+ (end - begin));
		reduce.print();
		reduce.writeAsText("D:\\monitor")

		;
        /*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */
		// execute program
		env.execute("Flink Batch Java API Skeleton");
	}
}
