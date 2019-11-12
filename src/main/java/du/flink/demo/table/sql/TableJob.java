package du.flink.demo.table.sql;


import com.alibaba.fastjson.JSON;
import du.flink.demo.model.FaceData;
import du.flink.demo.model.dto.FaceDataDTO;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * TableJob
 * @author dxy
 * @date 2019/11/11 10:50
 */
public class TableJob {
	public static void main(String[] args) {
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

		DataSet<FaceDataDTO> faceDataDataSet = batchTableEnvironment.toDataSet(table, FaceDataDTO.class);
		try {
			faceDataDataSet.print();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
