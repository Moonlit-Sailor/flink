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
import com.alicloud.openservices.tablestore.model.Stream;
import com.alicloud.openservices.tablestore.model.StreamShard;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.model.ListStreamRequest;
import com.alicloud.openservices.tablestore.model.ListStreamResponse;
import com.alicloud.openservices.tablestore.model.DescribeStreamRequest;
import com.alicloud.openservices.tablestore.model.DescribeStreamResponse;
import com.alicloud.openservices.tablestore.model.GetShardIteratorRequest;
import com.alicloud.openservices.tablestore.model.GetShardIteratorResponse;
import com.alicloud.openservices.tablestore.model.GetStreamRecordRequest;
import com.alicloud.openservices.tablestore.model.GetStreamRecordResponse;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedRestoring;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;


public class FlinkOtsSource extends RichParallelSourceFunction<StreamRecord> implements
	CheckpointedFunction,
	CheckpointedRestoring<LinkedList<OtsStreamPartitionState>> {

	private String endPoint;
	private String accessKeyId;
	private String accessKey;
	private String instanceId;
	private String tableName;
	private SyncClient syncClient;

	/** Flag indicating whether the subtask is still running **/
	private volatile boolean running = true;

	/** The set of topic partitions that the source will read, with their initial offsets to start reading from.
	 * Each thread(subtask) has its own partitionToStartOffset Map
	 * Map[shardId -> shardIterator] **/
	private Map<String, String> partitionToStartOffset;

	private List<OtsStreamPartitionState> partitions;

	private transient ListState<OtsStreamPartitionState> offsetsStateForCheckpoint;

	/** The offsets to restore to, if the source restores state from a checkpoint **/
	private transient volatile List<OtsStreamPartitionState> restoredState;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkOtsSource.class);


	public FlinkOtsSource(
		String endPoint,
		String accessKeyId,
		String accessKey,
		String instanceId,
		String tableName) {
		this.endPoint = checkNotNull(endPoint);
		this.accessKeyId = checkNotNull(accessKeyId);
		this.accessKey = checkNotNull(accessKey);
		this.instanceId = checkNotNull(instanceId);
		this.tableName = checkNotNull(tableName);
	}


	@Override
	public void open(Configuration configuration) {
		syncClient = new SyncClient(endPoint, accessKeyId, accessKey, instanceId);
		ListStreamRequest listStreamRequest = new ListStreamRequest(tableName);
		syncClient.listStream(listStreamRequest);
		ListStreamResponse result = syncClient.listStream(listStreamRequest);
		List<Stream> streams = result.getStreams();
		String streamId = streams.get(0).getStreamId();
		if(streamId != null){
			DescribeStreamRequest desRequest = new DescribeStreamRequest(streamId);
			DescribeStreamResponse response = syncClient.describeStream(desRequest);

			partitions = new LinkedList<>();
			if (restoredState != null) {
				for (OtsStreamPartitionState partition : restoredState) {
					partitions.add(partition);
				}

				return;
			}
			// start offsets of all stream shards of current subtask(thread) to read
			initializeStartOffset(
				streamId,
				partitions,
				response.getShards(),
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks());

			if (!partitions.isEmpty()) {
				partitionToStartOffset = new HashMap<>(partitions.size());
				for (OtsStreamPartitionState partition : partitions) {
					if (partition.getOffset() == 0L) { // start from the beginning
						continue;
					}
					GetShardIteratorRequest getShardIterRequest =
						new GetShardIteratorRequest(
							partition.getStreamId(),
							partition.getStreamShard().getShardId());
					GetShardIteratorResponse shardIterResponse = syncClient.getShardIterator(getShardIterRequest);

					String iter = shardIterResponse.getShardIterator();
					GetStreamRecordRequest streamRecordRequest;

					long offset = partition.getOffset();
					int times = (int) (offset / Integer.MAX_VALUE);
					int mod = (int) (offset % Integer.MAX_VALUE);
					for (int i =0 ;i < times; ++i) {
						streamRecordRequest = new GetStreamRecordRequest(iter);
						streamRecordRequest.setLimit(Integer.MAX_VALUE);
						iter = syncClient.getStreamRecord(streamRecordRequest).getNextShardIterator();
					}
					streamRecordRequest = new GetStreamRecordRequest(iter);
					streamRecordRequest.setLimit(mod);
					partitionToStartOffset.put(
						partition.getStreamShard().getShardId(),
						syncClient.getStreamRecord(streamRecordRequest).getNextShardIterator());
				}
			}
		}
	}

	@Override
	public void run(SourceContext<StreamRecord> ctx) throws Exception {
		if (partitions == null) {
			throw new Exception("The partitions were not set for the reader");
		}
		String iter;
		List<StreamRecord> streamRecords;
		while (running) {
			if (!partitions.isEmpty()) {
				for (OtsStreamPartitionState partition : partitions) {
					if (partitionToStartOffset.containsKey(partition.getStreamShard().getShardId())) {
						iter = partitionToStartOffset.get(partition.getStreamShard().getShardId());
					}
					else {
						GetShardIteratorRequest getShardIterRequest =
							new GetShardIteratorRequest(
								partition.getStreamId(),
								partition.getStreamShard().getShardId());

						iter = syncClient.getShardIterator(getShardIterRequest).getShardIterator();
					}
					GetStreamRecordRequest request = new GetStreamRecordRequest(iter);
					GetStreamRecordResponse response = syncClient.getStreamRecord(request);
					streamRecords = response.getRecords();
					partitionToStartOffset.put(
						partition.getStreamShard().getShardId(),
						response.getNextShardIterator());
					for (int i = 0; i < streamRecords.size(); ++i) {
						synchronized (ctx.getCheckpointLock()) {
							ctx.collect(streamRecords.get(i));
							partition.setOffset(partition.getOffset() + 1);
						}
					}
				}

			} else {
				ctx.emitWatermark(new Watermark(Long.MAX_VALUE));

				// wait until this is canceled
				final Object waitLock = new Object();
				while (running) {
					try {
						//noinspection SynchronizationOnLocalVariableOrMethodParameter
						synchronized (waitLock) {
							waitLock.wait();
						}
					}
					catch (InterruptedException e) {
						if (!running) {
							// restore the interrupted state, and fall through the loop
							Thread.currentThread().interrupt();
						}
					}
				}
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	@Override
	public void close() throws Exception {
		// pretty much the same logic as cancelling
		try {
			cancel();
		} finally {
			super.close();
		}
	}

	private void initializeStartOffset(
		String streamId,
		List<OtsStreamPartitionState> partitions,
		List<StreamShard> shards,
		int indexOfThisSubtask,
		int numParallelSubtasks) {
		for (int i = 0; i < shards.size(); ++i) {
			if (i % numParallelSubtasks == indexOfThisSubtask) {
				partitions.add(new OtsStreamPartitionState(streamId, shards.get(i), 0L));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and restore
	// ------------------------------------------------------------------------

	@Override
	public void restoreState(LinkedList<OtsStreamPartitionState> state) throws Exception {
		LOG.info("{} (taskIdx={}) restoring offsets from an older version.",
			getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask());
		restoredState = (state != null && state.isEmpty()) ? null : state;

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} (taskIdx={}) restored offsets from an older Flink version: {}",
				getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask(), restoredState);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (!running) {
			LOG.debug("snapshotState() called on closed source");
		}
		else {
			offsetsStateForCheckpoint.clear();

			for (OtsStreamPartitionState partition : partitions) {
				offsetsStateForCheckpoint.add(partition);
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		OperatorStateStore stateStore = context.getOperatorStateStore();
		offsetsStateForCheckpoint =
			stateStore.getSerializableListState(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);

		if (context.isRestored()) {
			if (restoredState == null) {
				restoredState = new LinkedList<>();
				for (OtsStreamPartitionState partition : offsetsStateForCheckpoint.get()) {
					restoredState.add(partition);
				}

				LOG.info("Setting restore state in the FlinkOtsSource.");
				if (LOG.isDebugEnabled()) {
					LOG.debug("Using the following offsets: {}", restoredState);
				}
			}
			if (restoredState != null && restoredState.isEmpty()) {
				restoredState = null;
			}
		}
		else {
			LOG.info("No restore state for FlinkOtsSource.");
		}
	}

}
