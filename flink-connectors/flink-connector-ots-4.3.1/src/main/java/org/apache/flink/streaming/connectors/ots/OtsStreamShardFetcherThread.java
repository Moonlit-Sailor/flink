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
import com.alicloud.openservices.tablestore.model.StreamShard;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class OtsStreamShardFetcherThread extends Thread{

	private volatile boolean running = true;
	private int taskId;
	private int totalParallelism;
	private int updateInterval;
	private SyncClient syncClient;
	private String streamId;
	private AtomicReference<List<StreamShard>> updatedShards;

	public OtsStreamShardFetcherThread(
		int taskId,
		int totalParallelism,
		SyncClient client,
		String streamId,
		AtomicReference<List<StreamShard>> updatedShards) {
		this.taskId = taskId;
		this.totalParallelism = totalParallelism;
		this.syncClient = client;
		this.streamId = streamId;
		this.updatedShards = updatedShards;
		this.updateInterval = 60000;
	}

	public OtsStreamShardFetcherThread(
		int taskId,
		int totalParallelism,
		SyncClient client,
		String streamId,
		AtomicReference<List<StreamShard>> updatedShards,
		int interval) {
		this.taskId = taskId;
		this.totalParallelism = totalParallelism;
		this.syncClient = client;
		this.streamId = streamId;
		this.updatedShards = updatedShards;
		this.updateInterval = interval;
	}

	@Override
	public void run() {
		List<StreamShard> shardList;
		while (running) {
			try {
				Thread.sleep(updateInterval);
				DescribeStreamRequest request = new DescribeStreamRequest(streamId);
				shardList = syncClient.describeStream(request).getShards();
				Iterator<StreamShard> it = shardList.iterator();
				while (it.hasNext()) { // get all of shards that belong to this subtask
					StreamShard shard = it.next();
					if (shard.getShardId().hashCode() % totalParallelism != taskId) {
						it.remove();
					}
				}
				if (updatedShards.getAndSet(shardList) != null) {
					Thread.sleep(updateInterval);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void shutdown(){
		running = false;
	}
}
