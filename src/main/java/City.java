import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class City implements WritableComparable {
	private String name;
	private double population;
	
	public City(City c) {
		name = c.getName();
		population = c.getPopulation();
	}
	public City(String name, double pop) {
		this.name = name;
		this.population = pop;
	}
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getPopulation() {
		return population;
	}

	public void setPopulation(double population) {
		this.population = population;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(name);
		out.writeDouble(population);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		population = in.readDouble();
	}


	@Override
	public int compareTo(Object o) {
		City p = (City) o;
		if (name.equals( p.getName()) && population == p.getPopulation())
			return 1;
		else
			return 0;
	}
	
	 public String toString() {
		    return name+ ": "+ Double.toString(population) ;
	 }
}
