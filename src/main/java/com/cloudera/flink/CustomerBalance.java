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

public class CustomerBalance {

	public long customer_id;
	public int balance = 1000;

	public CustomerBalance() {
	}

	public CustomerBalance(long customer_id, int balance) {
		this.customer_id = customer_id;
		this.balance = balance;
	}

	@Override
	public String toString() {
		return "CustomerBalance{" +
				"customer_id=" + customer_id +
				", balance=" + balance +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CustomerBalance that = (CustomerBalance) o;
		return customer_id == that.customer_id &&
				balance == that.balance;
	}

	@Override
	public int hashCode() {
		return Objects.hash(customer_id, balance);
	}
}
