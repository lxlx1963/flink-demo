package du.flink.batch;

/**
 * @author dxy
 * @date 2019/8/17 12:33
 */
public class AdvertisementMonitorData {
    private String advertisementName;
    private Integer deviceType;
    private String dateTime;
    private Integer sex;
    private Integer watchNumber;
    private String deviceNumber;
    private Double duration;
    private Integer touchNumber;
    private Integer playDuration;
    private String advertisementCode;
    private Integer screenType;
    private String playTime;
    private String projectName;
    private Double exposuresNumber;
    private String age;

    public String getAdvertisementName() {
        return advertisementName;
    }

    public void setAdvertisementName(String advertisementName) {
        this.advertisementName = advertisementName;
    }

    public Integer getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(Integer deviceType) {
        this.deviceType = deviceType;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public Integer getWatchNumber() {
        return watchNumber;
    }

    public void setWatchNumber(Integer watchNumber) {
        this.watchNumber = watchNumber;
    }

    public String getDeviceNumber() {
        return deviceNumber;
    }

    public void setDeviceNumber(String deviceNumber) {
        this.deviceNumber = deviceNumber;
    }

    public Double getDuration() {
        return duration;
    }

    public void setDuration(Double duration) {
        this.duration = duration;
    }

    public Integer getTouchNumber() {
        return touchNumber;
    }

    public void setTouchNumber(Integer touchNumber) {
        this.touchNumber = touchNumber;
    }

    public Integer getPlayDuration() {
        return playDuration;
    }

    public void setPlayDuration(Integer playDuration) {
        this.playDuration = playDuration;
    }

    public String getAdvertisementCode() {
        return advertisementCode;
    }

    public void setAdvertisementCode(String advertisementCode) {
        this.advertisementCode = advertisementCode;
    }

    public Integer getScreenType() {
        return screenType;
    }

    public void setScreenType(Integer screenType) {
        this.screenType = screenType;
    }

    public String getPlayTime() {
        return playTime;
    }

    public void setPlayTime(String playTime) {
        this.playTime = playTime;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public Double getExposuresNumber() {
        return exposuresNumber;
    }

    public void setExposuresNumber(Double exposuresNumber) {
        this.exposuresNumber = exposuresNumber;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "AdvertisementMonitorData{" +
                "advertisementName='" + advertisementName + '\'' +
                ", deviceType=" + deviceType +
                ", dateTime='" + dateTime + '\'' +
                ", sex=" + sex +
                ", watchNumber=" + watchNumber +
                ", deviceNumber='" + deviceNumber + '\'' +
                ", duration=" + duration +
                ", touchNumber=" + touchNumber +
                ", playDuration=" + playDuration +
                ", advertisementCode='" + advertisementCode + '\'' +
                ", screenType=" + screenType +
                ", playTime='" + playTime + '\'' +
                ", projectName='" + projectName + '\'' +
                ", exposuresNumber=" + exposuresNumber +
                ", age='" + age + '\'' +
                '}';
    }
}
