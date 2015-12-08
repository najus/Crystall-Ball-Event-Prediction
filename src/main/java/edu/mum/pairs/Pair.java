package edu.mum.pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

	private String key;
	private String value;

	public Pair() {

	}

	public Pair(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int compareTo(Pair o) {
		return this.key.compareTo(o.key);
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

}
