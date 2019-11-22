package du.flink.demo.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件类
 *
 * @author dxy
 * @date 2019/11/22 15:15
 */
public final class PropertiesUitls {
	/**
	 * 日志对象
	 */
	private static Logger logger = LoggerFactory.getLogger(PropertiesUitls.class);
	/**
	 * Properties
	 */
	private static final Properties properties = new Properties();
	/**
	 * 配置文件名称
	 */
	private static final String PROPERTIES_FILE_NAME = "application.properties";
	/**
	 * 错误提示信息
	 */
	private static final String INIT_ERROR_MESSAGE = "PropertiesUitls 加载配置文件失败";


	/**
	 * 静态加载
	 */
	static {
		InputStream inputStream = ClassLoader.getSystemResourceAsStream(PROPERTIES_FILE_NAME);
		ClassLoader.getSystemClassLoader().getResource("");
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			logger.error(INIT_ERROR_MESSAGE, e);
		}
	}

	/**
	 * 获取Properties对象
	 *
	 * @return Properties
	 */
	public static Properties getProperties() {
		return properties;
	}

	/**
	 * 获取key对应的值
	 *
	 * @param key 键
	 * @return String
	 */
	public static String getValueByKey(String key) {
		return properties.getProperty(key);
	}
}
