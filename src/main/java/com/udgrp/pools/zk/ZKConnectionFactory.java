

package com.udgrp.pools.zk;

import com.udgrp.pools.base.ConnectionFactory;
import com.udgrp.zktools.MyZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Created by kejw on 2017/10/14.
 */
public class ZKConnectionFactory implements ConnectionFactory<ZkUtils> {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 8271607382718512399L;


    private String zkServers;
    private int sessionTimeout;
    private int connectionTimeout;
    private ZkSerializer zkSerializer;
    private ZkClient zkClient;

    public ZKConnectionFactory(String zkServers, int sessionTimeout, int connectionTimeout) {
        this.zkServers = zkServers;
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
        this.zkSerializer = new MyZKStringSerializer();
        this.zkClient = new ZkClient(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
    }

    @Override
    public ZkUtils createConnection() throws Exception {
        ZkUtils zkUtils = ZkUtils.apply(this.zkClient, false);
        return zkUtils;
    }

    @Override
    public PooledObject<ZkUtils> makeObject() throws Exception {
        ZkUtils zkUtils = this.createConnection();
        return new DefaultPooledObject<ZkUtils>(zkUtils);
    }

    @Override
    public void destroyObject(PooledObject<ZkUtils> pooledObject) throws Exception {
        ZkUtils zkUtils = pooledObject.getObject();

        if (zkUtils != null) {
            zkUtils.close();
        }
    }

    @Override
    public boolean validateObject(PooledObject<ZkUtils> pooledObject) {
        ZkUtils zkUtils = pooledObject.getObject();
        if (zkUtils != null) {
            return true;
        }
        return false;
    }

    @Override
    public void activateObject(PooledObject<ZkUtils> pooledObject) throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<ZkUtils> pooledObject) throws Exception {

    }
}
