

package com.udgrp.pools.base;


import org.apache.commons.pool2.PooledObjectFactory;

import java.io.Serializable;

/**
 * Created by kejw on 2017/5/23.
 */
public interface ConnectionFactory<T> extends PooledObjectFactory<T>, Serializable {

    public abstract T createConnection() throws Exception;
}
