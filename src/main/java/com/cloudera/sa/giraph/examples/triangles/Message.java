/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sa.giraph.examples.triangles;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class Message implements Writable {

	public enum Type {

		DEGREE_UPDATE(0), OPEN_TRIAD(1), TRIANGLE(2);

		private int id;
		private Type(int id) {
			this.id = id;
		}
		public int getId() {
			return id;
		}
		public static Type getType(int id) {
			switch (id) {
			case 0:
				return DEGREE_UPDATE;
			case 1:
				return OPEN_TRIAD;
			case 2:
				return TRIANGLE;
			default:
				throw new IllegalArgumentException();
			}
		}
	}

	private Type type;

	// Degree Update
	private LongWritable source;
	private IntWritable degree;

	// Triad
	private LongWritable triadA;
	private LongWritable triadB;

	// Triangle
	private LongWritable triangleA;
	private LongWritable triangleB;
	private LongWritable triangleC;
	
	public Message() {}

	public Message(LongWritable source, IntWritable degree) {
		this.type = Type.DEGREE_UPDATE;
		this.source = new LongWritable(source.get());
		this.degree = new IntWritable(degree.get());
	}

	public Message(LongWritable triadA, LongWritable triadB) {
		this.type = Type.OPEN_TRIAD;
		this.triadA = new LongWritable(triadA.get());
		this.triadB = new LongWritable(triadB.get());
	}
	
	public Message(LongWritable triangleA, LongWritable triangleB, LongWritable triangleC) {
		this.type = Type.TRIANGLE;
		this.triangleA = new LongWritable(triangleA.get());
		this.triangleB = new LongWritable(triangleB.get());
		this.triangleC = new LongWritable(triangleC.get());
	}

	// Degree Update

	public LongWritable getSource() {
		return source;
	}

	public void setSource(LongWritable source) {
		this.source = source;
	}

	public IntWritable getDegree() {
		return degree;
	}

	public void setDegree(IntWritable degree) {
		this.degree = degree;
	}

	// Triad

	public LongWritable getTriadA() {
		return triadA;
	}

	public void setTriadA(LongWritable triadA) {
		this.triadA = triadA;
	}

	public LongWritable getTriadB() {
		return triadB;
	}

	public void setTriadB(LongWritable triadB) {
		this.triadB = triadB;
	}

	// Triangle
	
	public LongWritable getTriangleA() {
		return triangleA;
	}

	public void setTriangleA(LongWritable triangleA) {
		this.triangleA = triangleA;
	}

	public LongWritable getTriangleB() {
		return triangleB;
	}

	public void setTriangleB(LongWritable triangleB) {
		this.triangleB = triangleB;
	}

	public LongWritable getTriangleC() {
		return triangleC;
	}

	public void setTriangleC(LongWritable triangleC) {
		this.triangleC = triangleC;
	}

	// Writable methods
	
	public void readFields(DataInput in) throws IOException {
		type = Type.getType(in.readInt());
		if(type.equals(Type.DEGREE_UPDATE)) {
			source = new LongWritable();
			source.readFields(in);
			degree = new IntWritable();
			degree.readFields(in);
		}
		if(type.equals(Type.OPEN_TRIAD)) {
			triadA = new LongWritable();
			triadA.readFields(in);
			triadB = new LongWritable();
			triadB.readFields(in);
		}
		if(type.equals(Type.TRIANGLE)) {
			triangleA = new LongWritable();
			triangleA.readFields(in);
			triangleB = new LongWritable();
			triangleB.readFields(in);
			triangleC = new LongWritable();
			triangleC.readFields(in);
		}

	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(type.getId());
		if(type.equals(Type.DEGREE_UPDATE)) {
			source.write(out);
			degree.write(out);
		}
		if(type.equals(Type.OPEN_TRIAD)) {
			triadA.write(out);
			triadB.write(out);
		}
		if(type.equals(Type.TRIANGLE)) {
			triangleA.write(out);
			triangleB.write(out);
			triangleC.write(out);
		}
	}

	public String toString() {
		switch(type) {
		case DEGREE_UPDATE:
			return "Node " + source + " has a degree of " + degree;
		case OPEN_TRIAD:
			return "Do you know both " + triadA + " and " + triadB + "?";
		case TRIANGLE:
			return "Triangle (" + triangleA + "," + triangleB + "," + triangleC + ")";
		default:
			return "";
		}
	}

}
