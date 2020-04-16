/*
 * Licensed to Cloudera, Inc. under one
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

package com.cloudera.flink;

import java.util.Objects;

public class RetailTransaction {

	public long timestamp_ms;
	public String created_at;
	public int amount;
	public String text;
	public long  id_customer;

	public RetailTransaction() {
	}

	public RetailTransaction(long timestamp_ms, String created_at, int amount, String text, long id_customer) {
		this.timestamp_ms = timestamp_ms;
		this.created_at = created_at;
		this.amount = amount;
		this.text = text;
		this.id_customer = id_customer;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RetailTransaction that = (RetailTransaction) o;
		return timestamp_ms == that.timestamp_ms &&
				amount == that.amount &&
				id_customer == that.id_customer &&
				Objects.equals(created_at, that.created_at) &&
				Objects.equals(text, that.text);
	}

	@Override
	public int hashCode() {
		return Objects.hash(timestamp_ms, created_at, amount, text, id_customer);
	}

	@Override
	public String toString() {
		return "RetailTransaction{" +
				"timestamp_ms=" + timestamp_ms +
				", created_at='" + created_at + '\'' +
				", amount=" + amount +
				", text='" + text + '\'' +
				", id_customer=" + id_customer +
				'}';
	}
}
