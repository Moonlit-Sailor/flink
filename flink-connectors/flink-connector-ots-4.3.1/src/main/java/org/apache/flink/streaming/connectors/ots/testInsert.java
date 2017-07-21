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


import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;




public class testInsert {
	public static void main(String[] args){
		String endPoint = "http://testDB.cn-shanghai.ots.aliyuncs.com";
		String accessKeyId = "LTAIbGcyxd7aM1CJ";
		String accessKey = "Uk0vevvK02TYhPd16A62pOjgMr9hOA";
		String instanceId = "testDB";
		SyncClient client = new SyncClient(endPoint, accessKeyId, accessKey, instanceId);
		BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

		PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
		primaryKeyBuilder.addPrimaryKeyColumn("id", PrimaryKeyValue.fromLong(1L));
		PrimaryKey primaryKey1 = primaryKeyBuilder.build();

		RowPutChange rowPutChange1 = new RowPutChange("score", primaryKey1);
		rowPutChange1.addColumn(new Column("name", ColumnValue.fromString("Mike")));
		rowPutChange1.addColumn(new Column("score", ColumnValue.fromLong(98L)));

		batchWriteRowRequest.addRowChange(rowPutChange1);

		PrimaryKeyBuilder primaryKeyBuilder2 = PrimaryKeyBuilder.createPrimaryKeyBuilder();
		primaryKeyBuilder2.addPrimaryKeyColumn("id", PrimaryKeyValue.fromLong(2L));
		PrimaryKey primaryKey2 = primaryKeyBuilder2.build();

		RowPutChange rowPutChange2 = new RowPutChange("score", primaryKey2);
		rowPutChange2.addColumn(new Column("name", ColumnValue.fromString("George")));
		rowPutChange2.addColumn(new Column("score", ColumnValue.fromLong(34L)));

		batchWriteRowRequest.addRowChange(rowPutChange2);

		BatchWriteRowResponse batchWriteRowResponse = client.batchWriteRow(batchWriteRowRequest);

		System.out.println("isAll success:" + batchWriteRowResponse.isAllSucceed());


		if (!batchWriteRowResponse.isAllSucceed()) {
			for (BatchWriteRowResponse.RowResult r : batchWriteRowResponse.getFailedRows()) {

				System.out.println("failure row:" +
					batchWriteRowRequest.getRowChange(r.getTableName(), r.getIndex()).getPrimaryKey());
				System.out.println("reason:" + r.getError());

			}
//			BatchWriteRowRequest retryRequest =
//				batchWriteRowRequest.createRequestForRetry(batchWriteRowResponse.getFailedRows());
		}
//    else {
//      val result = batchWriteRowResponse.getRowStatus("score").asScala
//      result.foreach(re => println(re.getRow.toString))
//    }

		client.shutdown();
	}
}
