

package com.udgrp.pools.zk;

import com.udgrp.pools.base.ConnectionPool;
import com.udgrp.pools.base.PoolBase;
import com.udgrp.pools.base.PoolConfig;
import kafka.utils.ZkUtils;

public class ZKConnectionPool extends PoolBase<ZkUtils> implements ConnectionPool<ZkUtils> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -9126420905798370263L;

    public ZKConnectionPool() {
        this(ZKConfing.ZK_SERVERS);
    }

    public ZKConnectionPool(final String zkServers) {
        this(new PoolConfig(), zkServers);
    }

    /**
     * @param poolConfig
     */
    public ZKConnectionPool(final PoolConfig poolConfig, final String zkServers) {
        super(poolConfig, new ZKConnectionFactory(zkServers, ZKConfing.SESSION_TIMEOUT, ZKConfing.CONNECTION_TIMEOUT));
    }


    @Override
    public ZkUtils getConnection() {
        return super.getResource();
    }

    @Override
    public void returnConnection(ZkUtils conn) {
        super.returnResource(conn);
    }

    @Override
    public void invalidateConnection(ZkUtils conn) {
        super.invalidateResource(conn);
    }
}
