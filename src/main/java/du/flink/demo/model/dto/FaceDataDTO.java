package du.flink.demo.model.dto;

/**
 * FaceData
 *
 * @author dxy
 * @date 2019/11/5 10:11
 */
public class FaceDataDTO {

	public FaceDataDTO(){

	}

	public FaceDataDTO(String ageRange, Long total) {
		this.ageRange = ageRange;
		this.total = total;
	}

	/**
	 * 年龄段
	 */
	private String ageRange;
	/**
	 * 总数
	 */
	private Long total;

	public String getAgeRange() {
		return ageRange;
	}

	public void setAgeRange(String ageRange) {
		this.ageRange = ageRange;
	}

	public Long getTotal() {
		return total;
	}

	public void setTotal(Long total) {
		this.total = total;
	}

	@Override
	public String toString() {
		return "FaceDataDTO{" +
				"ageRange='" + ageRange + '\'' +
				", total=" + total +
				'}';
	}
}
