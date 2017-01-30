import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class RunnerWritable implements WritableComparable<Object> {
	private int  rank;
	private String lastname;
	private String firstname;
	private String clubName;
	private String raceName;
	private int distance;
	private long timeInSec;
	private Gender gender;
	private Category category;
	private int year;
	
	public RunnerWritable() {
		
	}
	
	public RunnerWritable(int rank, String lastname, String firstname, String clubName, String raceName, int distance,
			long timeInSec, Gender gender, Category category) {
		super();
		this.rank = rank;
		this.lastname = lastname;
		this.firstname = firstname;
		this.clubName = clubName;
		this.raceName = raceName;
		this.distance = distance;
		this.timeInSec = timeInSec;
		this.gender = gender;
		this.category = category;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getClubName() {
		return clubName;
	}

	public void setClubName(String clubName) {
		this.clubName = clubName;
	}

	public String getRaceName() {
		return raceName;
	}

	public void setRaceName(String raceName) {
		this.raceName = raceName;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public long getTimeInSec() {
		return timeInSec;
	}

	public void setTimeInSec(long timeInSec) {
		this.timeInSec = timeInSec;
	}

	public Gender getGender() {
		return gender;
	}

	public void setGender(Gender gender) {
		this.gender = gender;
	}

	public Category getCategory() {
		return category;
	}

	public void setCategory(Category category) {
		this.category = category;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(rank);
		out.writeUTF(lastname);
		out.writeUTF(firstname);
		out.writeUTF(clubName);
		out.writeUTF(raceName);
		out.writeInt(distance);
		out.writeLong(timeInSec);
		out.writeUTF(gender.toString());
		out.writeUTF(category.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		rank = in.readInt();
		lastname = in.readUTF();
		firstname = in.readUTF();
		clubName = in.readUTF();
		raceName = in.readUTF();
		distance = in.readInt();
		timeInSec = in.readLong();
		gender = Gender.valueOf(in.readUTF());
		category = Category.valueOf(in.readUTF());
	}


	@Override
	public int compareTo(Object o) {
		RunnerWritable p = (RunnerWritable) o;
		if (p.getFirstname().equals(this.firstname) && p.getLastname().equals(this.lastname) && p.getGender() == this.gender)
			return 1;
		else
			return 0;
	}

	@Override
	public String toString() {
		return "RunnerWritable [rank=" + rank + ", lastname=" + lastname + ", firstname=" + firstname + ", clubName="
				+ clubName + ", raceName=" + raceName + ", distance=" + distance + ", timeInSec=" + timeInSec
				+ ", gender=" + gender + ", category=" + category + "]";
	}

	public void setYear(int year) {
		this.year = year;
	}
	
	public int getYear() {
		return year;
	}
	
}
