package com.github.bigDataTools.zookeeper;

import java.util.List;

/**
 * @author winstone
 *
 */
public interface ChildListener {

    void childChanged(String path, List<String> children);

}