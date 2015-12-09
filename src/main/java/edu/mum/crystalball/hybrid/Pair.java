package edu.mum.crystalball.hybrid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

	private Text key;
	private Text value;

	public Pair() {
		set(new Text(), new Text());
	}
	
	public Pair(Text key, Text value) {
		this.key = key;
		this.value = value;
	}

	public Pair(String key, String value) {
		this.key = new Text(key);
		this.value = new Text(value);
	}
	
	public void set(Text key, Text value) {
		this.key = key;
		this.value = value;
	}

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}

	public int compareTo(Pair o) {
		int cmp = this.key.compareTo(o.key);
		 
        if (cmp != 0) {
            return cmp;
        }
 
        return this.value.compareTo(o.value);
	}

	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
	}

	@Override
	public String toString() {
		return "(" + key + ", " + value + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
}
