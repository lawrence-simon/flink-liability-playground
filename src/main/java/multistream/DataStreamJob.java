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

package multistream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TemporalTableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		Configuration config = new Configuration();
		config.setInteger("table.exec.source.idle-timeout", 2000);
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().withConfiguration(config).build();
//		EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();

		final TableEnvironment tEnv = TableEnvironment.create(settings);

		tEnv.executeSql("CREATE TABLE orders (\n" +
				"  `order_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
				"  `order_id` BIGINT,\n" +
				"  `currency` STRING,\n" +
				"  `order_value` FLOAT,\n" +
				" WATERMARK FOR order_time AS order_time - INTERVAL '2' SECOND" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'flink-table-orders',\n" +
				"  'properties.bootstrap.servers' = 'localhost:19092',\n" +
				"  'properties.group.id' = 'ordersGroup',\n" +
				"  'scan.startup.mode' = 'earliest-offset',\n" +
				"  'format' = 'json',\n" +
				"  'json.ignore-parse-errors' = 'true'\n" +
				")");

		//tEnv.sqlQuery("SELECT order_time, order_id, currency AS o_currency, order_value FROM orders").execute().print();

		tEnv.executeSql("CREATE TABLE currency_rates (\n" +
				" `update_time` TIMESTAMP(3) METADATA FROM 'timestamp', \n" +
				" `currency` STRING,\n" +
				" `rate` FLOAT,\n" +
				" WATERMARK FOR update_time AS update_time - INTERVAL '2' SECOND" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'flink-table-reference',\n" +
				"  'properties.bootstrap.servers' = 'localhost:19092',\n" +
				"  'properties.group.id' = 'referenceGroup',\n" +
				"  'scan.startup.mode' = 'earliest-offset',\n" +
				"  'format' = 'json',\n" +
				"  'json.ignore-parse-errors' = 'true'\n" +
				")");

		//tEnv.sqlQuery("SELECT update_time, currency, rate FROM currency_rates").execute().print();

		TemporalTableFunction rates = tEnv
				.from("currency_rates")
				.createTemporalTableFunction($("update_time"), $("currency"));

		tEnv.createTemporarySystemFunction("rates", rates);

		//tEnv.sqlQuery("SELECT r.* FROM LATERAL TABLE (rates(CURRENT_TIMESTAMP)) r").execute().print();

		tEnv.sqlQuery("SELECT o.order_time, o.order_id, o.order_value * r.rate AS amount\n" +
					"  FROM\n" +
					"  	orders o,\n" +
					"  	LATERAL TABLE (rates(o.order_time)) AS r\n" +
					"  WHERE r.currency = o.currency")
				.execute().print();

//		tEnv.sqlQuery("SELECT o.order_time, o.order_id, o.order_value * r.rate AS amount\n" +
//						"  FROM\n" +
//						"  	orders o,\n" +
//						"  	currency_rates AS r\n" +
//						"  WHERE r.currency = o.currency" +
//						"   AND o.order_time > r.update_time")
//				.execute().print();

//		orders
//			.leftOuterJoinLateral(call("rates", $("order_time")), $("o_currency").isEqual($("currency")))
//			.select(
//					$("order_value").times($("rate")).sum().as("amount")
//			)
//			.execute().print();
	}
}
