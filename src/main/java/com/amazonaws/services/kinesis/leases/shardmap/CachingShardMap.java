package com.amazonaws.services.kinesis.leases.shardmap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.Shard;
import com.google.common.base.Predicate;

import lombok.Data;
import lombok.extern.apachecommons.CommonsLog;

@CommonsLog
public class CachingShardMap implements IShardMap {

    private final SortedMap<String, ShardRelation> relationMap;
    private final String streamName;
    private final AmazonKinesis kinesisClient;

    public CachingShardMap(AmazonKinesis kinesisClient, String streamName) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.relationMap = new TreeMap<>();
    }

    @Override
    public synchronized SortedMap<String, Shard> getShardsFor(final String shardId) {
        Validate.notNull(shardId);
        if (relationMap.isEmpty()) {
            log.debug("Relationship map is empty, reading full shard map.");
            loadShardMap(new Predicate<DescribeStreamResult>() {
                @Override
                public boolean apply(DescribeStreamResult input) {
                    return isDescribeStreamComplete(input);
                }
            });
        }
        ShardRelation relation = relationMap.get(shardId);

        if (relation == null || !relation.isSatisfied()) {
            log.debug("Unable to satisfy requirements for " + shardId + " triggering load. Current Relation: "
                    + relation);
            loadShardMap(new Predicate<DescribeStreamResult>() {
                @Override
                public boolean apply(DescribeStreamResult input) {
                    ShardRelation innerRelation = relationMap.get(shardId);
                    return isDescribeStreamComplete(input) || (innerRelation != null && innerRelation.isSatisfied());
                }
            });
        }
        if (relation == null) {
            log.error("Failed to find any descendants for shard " + shardId);
            throw new IllegalStateException("Failed to find any descendants for shardId: " + shardId);
        }
        return relation.getChildren();
    }

    @Override
    public synchronized void removeShardsBefore(String shardId) {
        Validate.notNull(shardId);
        if (shardId.compareTo(relationMap.firstKey()) < 0) {
            //
            // Shard map doesn't contain any keys less than the give shardId
            //
            return;
        }
        SortedMap<String, ShardRelation> toRemove = relationMap.subMap(relationMap.firstKey(), shardId);
        log.debug("Removing " + toRemove.size() + " keys from the shard relations.");
        for (String key : toRemove.keySet()) {
            relationMap.remove(key);
        }
    }

    private boolean isDescribeStreamComplete(DescribeStreamResult result) {
        return result != null && !result.getStreamDescription().isHasMoreShards();
    }

    private void loadShardMap(Predicate<DescribeStreamResult> iterationComplete) {
        String lastKey = null;
        DescribeStreamResult describeStreamResult = null;
        do {
            DescribeStreamRequest request = new DescribeStreamRequest().withStreamName(streamName)
                    .withExclusiveStartShardId(lastKey);
            try {
                describeStreamResult = kinesisClient.describeStream(request);
            } catch (LimitExceededException le) {
                //
                // Backoff
                //
                continue;
            }
            for (Shard shard : describeStreamResult.getStreamDescription().getShards()) {
                ShardRelation relation = new ShardRelation(shard);
                for (String parentShardId : relation.getParentShardIds()) {
                    if (!relationMap.containsKey(parentShardId)) {
                        relationMap.put(parentShardId, relation);
                    }
                    relationMap.get(parentShardId).merge(relation);
                }

                if (lastKey == null || shard.getShardId().compareTo(lastKey) > 0) {
                    lastKey = shard.getShardId();
                }
            }
        } while (!iterationComplete.apply(describeStreamResult));

    }

    @Data
    private class ShardRelation {

        public ShardRelation(Shard shard) {
            if (StringUtils.isNotEmpty(shard.getParentShardId())) {
                if (StringUtils.isNotEmpty(shard.getAdjacentParentShardId())) {
                    this.parentShardIds = Arrays.asList(shard.getParentShardId(), shard.getAdjacentParentShardId());
                } else {
                    this.parentShardIds = Collections.singletonList(shard.getParentShardId());
                }
            } else {
                this.parentShardIds = Collections.emptyList();
            }
            this.childShards = new TreeMap<>();
            this.childShards.put(shard.getShardId(), shard);
        }

        private final List<String> parentShardIds;
        private final SortedMap<String, Shard> childShards;

        public void merge(ShardRelation other) {
            childShards.putAll(other.getChildShards());
        }

        public boolean isSatisfied() {
            return (parentShardIds.size() == 2 && !childShards.isEmpty()) || childShards.size() >= 2;
        }

        public SortedMap<String, Shard> getChildren() {
            if (!isSatisfied()) {
                throw new IllegalStateException(
                        "Can't satisfy relationship for parent shard(s): [" + StringUtils.join(parentShardIds, ", ")
                                + "] to child shard(s): [" + StringUtils.join(childShards.keySet(), ", ") + "]");
            }
            return childShards;
        }
    }

}
