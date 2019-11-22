package du.flink.demo.table.sql;

import com.alibaba.fastjson.JSON;
import du.flink.demo.constant.ApplicationPropertiesContstant;
import du.flink.demo.constant.DateConstant;
import du.flink.demo.model.FaceData;
import du.flink.demo.model.dto.FaceDataDTO;
import du.flink.demo.util.DateUtils;
import du.flink.demo.util.PropertiesUitls;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author dxy
 * @date 2019/11/22 15:48
 */
public class PeopleNumberMySQLTableJob {
	public static void main(String[] args) {
		try {
			// 执行环境
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			// 文件路径
			String filePath = "hdfs://dev-base-bigdata-bj1-01:8020/flume/face/";
			filePath = "E:\\face";

			// 读取文件，获得DataSource
			DataSource<String> dataSource = env.readTextFile(filePath);

			// 转化数据
			MapOperator<String, FaceData> faceDataMap = dataSource.map(faceDataStr -> JSON.parseObject(faceDataStr, FaceData.class));

			BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);

			batchTableEnvironment.registerDataSet("FaceData", faceDataMap);

//		String sql = "SELECT ageRange,COUNT(id) as total FROM FaceData GROUP BY ageRange";
			String sql = "SELECT face.ageRange,COUNT(face.total) as total FROM (SELECT ageRange,visitorId as total FROM FaceData GROUP BY ageRange,visitorId) face GROUP BY face.ageRange";
			Table table = batchTableEnvironment.sqlQuery(sql);

			String insertSql = "insert into frp_test_people_number(year_month_day, age_range, people_number, add_time) values (?,?,?,?)";

			String url = PropertiesUitls.getValueByKey(ApplicationPropertiesContstant.MYSQL_DASHBOARD_URL);
			String username = PropertiesUitls.getValueByKey(ApplicationPropertiesContstant.MYSQL_DASHBOARD_USERNAME);
			String password = PropertiesUitls.getValueByKey(ApplicationPropertiesContstant.MYSQL_DASHBOARD_PASSWORD);
			String driverClassName = PropertiesUitls.getValueByKey(ApplicationPropertiesContstant.MYSQL_DASHBOARD_DRIVER_CLASS_NAME);

			String yearMonthDay = DateUtils.getPastDay(0, DateConstant.DATE_YEAR_MONTH_DAY);

			batchTableEnvironment.toDataSet(table, FaceDataDTO.class).map(faceDataDTO -> {
				Row row = new Row(4);
				row.setField(0, yearMonthDay);
				row.setField(1, faceDataDTO.getAgeRange());
				row.setField(2, faceDataDTO.getTotal());
				row.setField(3, DateUtils.getCurrentTimestamp());
				return row;
			}).output(JDBCOutputFormat.buildJDBCOutputFormat()
					.setDBUrl(url)
					.setUsername(username)
					.setPassword(password)
					.setDrivername(driverClassName)
					.setQuery(insertSql)
					.setBatchInterval(1000)
					.finish());

			env.execute("");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
