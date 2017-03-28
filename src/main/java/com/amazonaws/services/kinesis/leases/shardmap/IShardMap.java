package com.amazonaws.services.kinesis.leases.shardmap;

import java.util.SortedMap;

import com.amazonaws.services.kinesis.model.Shard;

public interface IShardMap {

    /**
     * Retrieves the shards that were created when the given shardId was closed
     * 
     * @param shardId
     *            the id of the shard to retrieve children for
     * @return a SortedMap of the children. This can consist of 0 to 2 shards depending on the event
     * @throws IllegalArgumentException
     *             if shardId is null
     * @throws IllegalStateException
     *             if it's not possible to produce a sane set of shards for the given shard.
     */
    SortedMap<String, Shard> getShardsFor(String shardId);

    /**
     * Removes all shards that are lexicographically less than the given shard id.
     * 
     * @param shardId
     *            the lowest shard id, that is still alive.
     */
    void removeShardsBefore(String shardId);

}
