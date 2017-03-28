package com.amazonaws.services.kinesis.leases.shardmap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;

@RunWith(MockitoJUnitRunner.class)
public class CachingShardMapTest {

    @Mock
    private AmazonKinesis kinesisClient;
    @Mock
    private DescribeStreamResult describeStreamResult;
    @Mock
    private StreamDescription streamDescription;

    private static final String STREAM_NAME = "TestStream";

    private static final String SPLIT_PARENT_SHARD_ID = "ShardId-100";
    private static final String CHILD_SHARD_ID_1 = "ShardId-101";
    private static final String CHILD_SHARD_ID_2 = "ShardId-102";

    private static final String MERGE_PARENT_SHARD_ID_1 = "ShardId-103";
    private static final String MERGE_PARENT_SHARD_ID_2 = "ShardId-104";
    private static final String MERGED_SHARD_ID = "ShardId-105";

    private static final String DEAD_SHARD_ID = "ShardId-000";
    private static final String DYING_SHARD_ID = "ShardId-010";
    private static final String NEXT_TO_DYING_SHARD_ID = "ShardId-011";

    @Test
    public void testShardSplitReturnsTwoShards() {
        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResult);
        when(describeStreamResult.getStreamDescription()).thenReturn(streamDescription);
        ShardListBuilder slb = slb().split(SPLIT_PARENT_SHARD_ID, CHILD_SHARD_ID_1, CHILD_SHARD_ID_2);
        when(streamDescription.getShards()).thenReturn(slb.toList());
        when(streamDescription.isHasMoreShards()).thenReturn(false);

        CachingShardMap shardMap = new CachingShardMap(kinesisClient, STREAM_NAME);

        SortedMap<String, Shard> expected = slb.mapFor(CHILD_SHARD_ID_1, CHILD_SHARD_ID_2);

        SortedMap<String, Shard> actual = shardMap.getShardsFor(SPLIT_PARENT_SHARD_ID);

        assertThat(actual, equalTo(expected));

        SortedMap<String, Shard> actualCache = shardMap.getShardsFor(SPLIT_PARENT_SHARD_ID);
        assertThat(actualCache, equalTo(expected));

        verify(kinesisClient).describeStream(any(DescribeStreamRequest.class));
    }

    @Test
    public void testShardMergeReturnsSingleShard() {
        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResult);
        when(describeStreamResult.getStreamDescription()).thenReturn(streamDescription);
        ShardListBuilder slb = slb().merge(MERGE_PARENT_SHARD_ID_1, MERGE_PARENT_SHARD_ID_2, MERGED_SHARD_ID);
        when(streamDescription.getShards()).thenReturn(slb.toList());
        when(streamDescription.isHasMoreShards()).thenReturn(false);

        CachingShardMap shardMap = new CachingShardMap(kinesisClient, STREAM_NAME);

        SortedMap<String, Shard> expected = slb.mapFor(MERGED_SHARD_ID);

        SortedMap<String, Shard> actualForShardId1 = shardMap.getShardsFor(MERGE_PARENT_SHARD_ID_1);
        assertThat(actualForShardId1, equalTo(expected));

        SortedMap<String, Shard> actualForShardId2 = shardMap.getShardsFor(MERGE_PARENT_SHARD_ID_2);
        assertThat(actualForShardId2, equalTo(expected));

    }

    @Test(expected = IllegalStateException.class)
    public void testNoLongerAliveShard() {
        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResult);
        when(describeStreamResult.getStreamDescription()).thenReturn(streamDescription);
        ShardListBuilder slb = slb().merge(MERGE_PARENT_SHARD_ID_1, MERGE_PARENT_SHARD_ID_2, MERGED_SHARD_ID);
        when(streamDescription.getShards()).thenReturn(slb.toList());
        when(streamDescription.isHasMoreShards()).thenReturn(false);
        CachingShardMap shardMap = new CachingShardMap(kinesisClient, STREAM_NAME);
        shardMap.getShardsFor(DEAD_SHARD_ID);
    }

    @Test(expected = IllegalStateException.class)
    public void testShardRemovedCorrectly() {
        CachingShardMap shardMap = new CachingShardMap(kinesisClient, STREAM_NAME);
        SortedMap<String, Shard> expected = createMap(MERGED_SHARD_ID);
        SortedMap<String, Shard> actual = null;
        try {
            actual = shardMap.getShardsFor(DYING_SHARD_ID);
        } catch (IllegalArgumentException iae) {
            Assert.fail("Should not have received an IllegalArgumentException while requesting " + DYING_SHARD_ID);
        }
        assertThat(actual, equalTo(expected));

        shardMap.removeShardsBefore(NEXT_TO_DYING_SHARD_ID);
        shardMap.getShardsFor(DYING_SHARD_ID);
    }

    private SortedMap<String, Shard> createMap(String... shardIds) {
        SortedMap<String, Shard> result = new TreeMap<>();
        for (String shardId : shardIds) {
            result.put(shardId, new Shard().withShardId(shardId));
        }
        return result;
    }

    private ShardListBuilder slb() {
        return new ShardListBuilder();
    }

    private static class ShardListBuilder {
        SortedMap<String, Shard> shards = new TreeMap<>();

        ShardListBuilder split(String parent, String child1, String child2) {
            shards.put(parent, new Shard().withShardId(parent));
            shards.put(child1, new Shard().withShardId(child1).withParentShardId(parent));
            shards.put(child2, new Shard().withShardId(child2).withParentShardId(parent));
            return this;
        }

        ShardListBuilder merge(String parent1, String parent2, String child) {
            shards.put(parent1, new Shard().withShardId(parent1));
            shards.put(parent2, new Shard().withShardId(parent2));
            shards.put(child,
                    new Shard().withShardId(child).withParentShardId(parent1).withAdjacentParentShardId(parent2));
            return this;
        }

        ShardListBuilder unrelated(String shard) {
            shards.put(shard, new Shard().withShardId(shard));
            return this;
        }

        List<Shard> toList() {
            return new ArrayList<>(shards.values());
        }

        SortedMap<String, Shard> mapFor(String... keys) {
            SortedMap<String, Shard> result = new TreeMap<>();
            for (String key : keys) {
                result.put(key, shards.get(key));
            }

            return result;
        }
    }

}