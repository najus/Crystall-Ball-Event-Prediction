package edu.mum.pairs;

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
		return this.key.compareTo(o.key);
	}

	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
	}

}
