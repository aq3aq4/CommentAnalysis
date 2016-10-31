package cwh.posting.kafka.domain;

public class PeopleDomain {
	private String gender;
	private String team;
	private String email;
	private int age;
	
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getTeam() {
		return team;
	}
	public void setTeam(String team) {
		this.team = team;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	@Override
	public String toString() {
		return "gender : " + gender + " team : " + team + " email : " + email + " age : " + age;
	}
	
	
}
