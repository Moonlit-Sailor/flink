package org.apache.flink.streaming.connectors.ots;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.alicloud.openservices.tablestore.model.StreamRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class testFlinkSource {
	public static void main(String[] args){
		String endPoint = "http://testDB.cn-shanghai.ots.aliyuncs.com";
		String accessKeyId = "LTAIbGcyxd7aM1CJ";
		String accessKey = "Uk0vevvK02TYhPd16A62pOjgMr9hOA";
		String instanceId = "testDB";
		String tableName = "score";
		String stateTable = "streamShardState";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<StreamRecord> source =
			env.addSource(
				new FlinkOtsSource(endPoint, accessKeyId, accessKey, instanceId, tableName, stateTable));
		DataStream<Long> age = source.map(new MapFunction<StreamRecord, Long>() {
			@Override
			public Long map(StreamRecord value) throws Exception {
				return value.getColumns().get(1).getColumn().getValue().asLong();
			}
		});
		age.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
