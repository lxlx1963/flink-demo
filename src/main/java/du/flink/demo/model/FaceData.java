package du.flink.demo.model;

/**
 * FaceData
 *
 * @author dxy
 * @date 2019/11/5 10:11
 */
public class FaceData {

	public FaceData(){

	}

	public FaceData(String ageRange, Long enterTime, String dateTime, String gender, Long id, Long visitorId) {
		this.ageRange = ageRange;
		this.enterTime = enterTime;
		this.dateTime = dateTime;
		this.gender = gender;
		this.id = id;
		this.visitorId = visitorId;
	}

	/**
	 * 年龄段
	 */
	private String ageRange;
	/**
	 * 进入时间
	 */
	private Long enterTime;
	/**
	 * 日期时间
	 */
	private String dateTime;
	/**
	 * 性别
	 */
	private String gender;
	/**
	 * id
	 */
	private Long id;
	/**
	 * visitorId
	 */
	private Long visitorId;


	public String getAgeRange() {
		return ageRange;
	}

	public void setAgeRange(String ageRange) {
		this.ageRange = ageRange;
	}

	public Long getEnterTime() {
		return enterTime;
	}

	public void setEnterTime(Long enterTime) {
		this.enterTime = enterTime;
	}

	public String getDateTime() {
		return dateTime;
	}

	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getVisitorId() {
		return visitorId;
	}

	public void setVisitorId(Long visitorId) {
		this.visitorId = visitorId;
	}

	@Override
	public String toString() {
		return "FaceData{" +
				"ageRange='" + ageRange + '\'' +
				", enterTime=" + enterTime +
				", dateTime='" + dateTime + '\'' +
				", gender='" + gender + '\'' +
				", id=" + id +
				", visitorId=" + visitorId +
				'}';
	}
}
