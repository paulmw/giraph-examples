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

package com.cloudera.sa.giraph.examples.ktrusses;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class EdgeState implements Writable {

	private int targetDegree;
	private int support;
	private boolean active;
	
	public EdgeState() {}
	
	public EdgeState(int targetDegree, int support, boolean active) {
		this.targetDegree = targetDegree;
		this.support = support;
		this.active = active;
	}
	
	public int getTargetDegree() {
		return targetDegree;
	}
	public void setTargetDegree(int targetDegree) {
		this.targetDegree = targetDegree;
	}
	public int getSupport() {
		return support;
	}
	public void setSupport(int support) {
		this.support = support;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}

	public void readFields(DataInput in) throws IOException {
		targetDegree = in.readInt();
		support = in.readInt();
		active = in.readBoolean();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(targetDegree);
		out.writeInt(support);
		out.writeBoolean(active);
		
	}
	
	public String toString() {
		return "(" + targetDegree + "," + support + "," + active + ")";
	}
	
}
