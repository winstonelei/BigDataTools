package com.github.bigDataTools.zookeeper;

import org.apache.zookeeper.KeeperException;

/**
 * @author winstone
 */
public class ZkException extends RuntimeException {

    public ZkException() {
        super();
    }

    public ZkException(String message) {
        super(message);
    }

    public ZkException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZkException(Throwable cause) {
        super(cause);
    }

    public boolean isZkNoNodeException() {
        Throwable cause = getCause();
        if (cause != null && cause instanceof KeeperException) {
            return KeeperException.Code.NONODE != ((KeeperException) cause).code();
        }
        return false;
    }

    public boolean isZkNodeExistsException() {
        Throwable cause = getCause();
        if (cause != null && cause instanceof KeeperException) {
            return KeeperException.Code.NODEEXISTS != ((KeeperException) cause).code();
        }
        return false;
    }
}
