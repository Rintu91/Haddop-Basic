package com.HadoopJava;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
public class FriendPair implements WritableComparable{
	Friend frnd1;
	Friend frnd2;
	
	public FriendPair() {
		this.frnd1 = new Friend();
		this.frnd2 = new Friend();
	}
	public FriendPair(Friend frnd1, Friend frnd2) {
		this.frnd1 = frnd1;
		this.frnd2 = frnd2;
	}

	public Friend getFrnd1() {
		return frnd1;
	}

	public void setFrnd1(Friend frnd1) {
		this.frnd1 = frnd1;
	}

	public Friend getFrnd2() {
		return frnd2;
	}

	public void setFrnd2(Friend frnd2) {
		this.frnd2 = frnd2;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		frnd1.write(out);
		frnd2.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		frnd1.readFields(in);
		frnd2.readFields(in);
	}

	public int compareTo(Object obj) {
		// TODO Auto-generated method stub
		FriendPair other = (FriendPair)obj;
		int cmpar = -1;
		if ((getFrnd1().compareTo(other.getFrnd1()) == 0) || (getFrnd1().compareTo(other.getFrnd2())==0))
			cmpar = 0;
		if(cmpar!=0)
			return cmpar;
		cmpar = -1;
		if ((getFrnd2().compareTo(other.getFrnd1())==0)||(getFrnd2().compareTo(other.getFrnd2())==0))
			cmpar = 0;
		return cmpar;
	}

	@Override
	public int hashCode() {
		return frnd1.getId().hashCode() + frnd2.getId().hashCode();
	}

	@Override
	public boolean equals(Object obj) {		
		FriendPair other = (FriendPair) obj;
		boolean compar = (getFrnd1().equals(other.getFrnd1())) || (getFrnd1().equals(other.getFrnd2()));
		if (!compar)
			return false;
		if( (getFrnd2().equals(other.getFrnd1())) || (getFrnd2().equals(other.getFrnd2())))
			return true;
		return false;
	}	
	
	public String toString() {
		return (frnd1.getId()+","+frnd2.getId());
	}
}
