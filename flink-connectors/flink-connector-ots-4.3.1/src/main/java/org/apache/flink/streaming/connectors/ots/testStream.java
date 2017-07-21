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
import com.alicloud.openservices.tablestore.model.DescribeStreamRequest;
import com.alicloud.openservices.tablestore.model.DescribeStreamResponse;
import com.alicloud.openservices.tablestore.model.ListStreamRequest;
import com.alicloud.openservices.tablestore.model.ListStreamResponse;
import com.alicloud.openservices.tablestore.model.GetShardIteratorRequest;
import com.alicloud.openservices.tablestore.model.GetShardIteratorResponse;
import com.alicloud.openservices.tablestore.model.GetStreamRecordRequest;
import com.alicloud.openservices.tablestore.model.GetStreamRecordResponse;
import com.alicloud.openservices.tablestore.model.Stream;
import com.alicloud.openservices.tablestore.model.StreamRecord;


import java.util.List;

public class testStream {
	public static void main(String[] args){
		String endPoint = "http://testDB.cn-shanghai.ots.aliyuncs.com";
		String accessKeyId = "LTAIbGcyxd7aM1CJ";
		String accessKey = "Uk0vevvK02TYhPd16A62pOjgMr9hOA";
		String instanceId = "testDB";
		SyncClient client = new SyncClient(endPoint, accessKeyId, accessKey, instanceId);

		ListStreamRequest listStreamRequest = new ListStreamRequest("score");
		ListStreamResponse result = client.listStream(listStreamRequest);
		List<Stream> streams = result.getStreams();
		String streamId = streams.get(0).getStreamId();
		if(streamId != null){
			DescribeStreamRequest desRequest = new DescribeStreamRequest(streamId);
			DescribeStreamResponse response = client.describeStream(desRequest);
			String firstShardId = response.getShards().get(0).getShardId();

			if(firstShardId != null) {
				GetShardIteratorRequest getShardIterRequest = new GetShardIteratorRequest(streamId, firstShardId);
				GetShardIteratorResponse shardIterResponse = client.getShardIterator(getShardIterRequest);
				String sIter = shardIterResponse.getShardIterator();

				GetStreamRecordRequest streamRecordRequest = new GetStreamRecordRequest(sIter);

				System.out.println(streamRecordRequest.getLimit()+streamRecordRequest.getShardIterator());
				streamRecordRequest.setLimit(1);
				System.out.println(streamRecordRequest.getLimit()+streamRecordRequest.getShardIterator());
				GetStreamRecordResponse streamRecordResponse = client.getStreamRecord(streamRecordRequest);
				System.out.println("next Iter:" + streamRecordResponse.getNextShardIterator());
				List<StreamRecord> records = streamRecordResponse.getRecords();

				System.out.println("curr Iter:" + sIter);
				System.out.println("next Iter:" + streamRecordResponse.getNextShardIterator());
				System.out.println("records size:" + records.size());
				for(StreamRecord record : records) {
					System.out.println(record);

				}


				System.out.println("next Iter:" + streamRecordResponse.getNextShardIterator());
				System.out.println("curr Iter:"+ shardIterResponse.getShardIterator());


				String nextIter = streamRecordResponse.getNextShardIterator();
				GetStreamRecordRequest streamRecordRequest2 = new GetStreamRecordRequest(nextIter);
				System.out.println(streamRecordRequest2.getLimit());
				streamRecordRequest2.setLimit(7);
				System.out.println(streamRecordRequest2.getLimit());
				GetStreamRecordResponse streamRecordResponse2 = client.getStreamRecord(streamRecordRequest2);
				System.out.println("next Iter:" + streamRecordResponse2.getNextShardIterator());
				List<StreamRecord> records2 = streamRecordResponse2.getRecords();

				System.out.println("records2 size:" + records2.size());
				for(StreamRecord record : records2)
					System.out.println(record);


				String nextIter3 = streamRecordResponse2.getNextShardIterator();
				GetStreamRecordRequest streamRecordRequest3 = new GetStreamRecordRequest(nextIter3);
				System.out.println(streamRecordRequest3.getLimit());
				streamRecordRequest3.setLimit(7);
				System.out.println(streamRecordRequest3.getLimit());
				GetStreamRecordResponse streamRecordResponse3 = client.getStreamRecord(streamRecordRequest3);
				System.out.println("next Iter:" + streamRecordResponse3.getNextShardIterator());
				List<StreamRecord> records3 = streamRecordResponse3.getRecords();

				System.out.println("records3 size:" + records3.size());
				for(StreamRecord record : records3)
					System.out.println(record);

			}

		}


		client.shutdown();

	}

}
