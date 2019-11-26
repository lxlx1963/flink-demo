package du.flink.demo.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * 配置文件类
 *
 * @author dxy
 * @date 2019/11/22 15:15
 */
public final class PropertiesUtils {
	/**
	 * 日志对象
	 */
	private static Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);
	/**
	 * Properties
	 */
	private static final Properties properties = new Properties();
	/**
	 * Profiles-Properties
	 */
	private static final Properties PROFILES_PROPERTIES = new Properties();
	/**
	 * 配置文件名称
	 */
	private static final String PROPERTIES_FILE_NAME = "application.properties";
	/**
	 * profiles-active
	 */
	private static final String PROFILES_ACTIVE = "profiles-active";
	/**
	 * 错误提示信息
	 */
	private static final String INIT_ERROR_MESSAGE = "PropertiesUtils 加载配置文件失败";

	/**
	 * 静态加载
	 */
	static {
		InputStream inputStream = ClassLoader.getSystemResourceAsStream(PROPERTIES_FILE_NAME);
		try {
			properties.load(inputStream);
			// 获取激活配置文件
			String profilesActive = properties.getProperty(PROFILES_ACTIVE);
			if (StringUtils.isBlank(profilesActive)) {
				throw new NullPointerException("请指定配置文件");
			}
			// 获取指定配置文件
			String applicationFileName = getApplicationFileName(profilesActive);
			if (StringUtils.isBlank(applicationFileName)) {
				throw new NullPointerException("配置文件不存在");
			}
			InputStream profilesInputStream = ClassLoader.getSystemResourceAsStream(applicationFileName);
			properties.load(profilesInputStream);
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

	/**
	 * 根据激活文件名获取激活配置文件
	 *
	 * @param profilesActive
	 * @return String
	 */
	private static String getApplicationFileName(String profilesActive) {
		// 获取resource根目录的URL
		URL url = ClassLoader.getSystemClassLoader().getResource("");
		// resource目录路径
		String path = url.getPath();
		// 创建文件
		File file = new File(path);

		// 获取目录下的所有文件及目录
		String[] list = file.list();
		for (String str : list) {
			if (str.contains(profilesActive)) {
				return str;
			}
		}
		return "";
	}
}
