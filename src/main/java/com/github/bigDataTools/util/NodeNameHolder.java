/**
 * 
 */
package com.github.bigDataTools.util;

import org.apache.commons.lang3.RandomStringUtils;

import java.net.InetAddress;
import java.util.UUID;

/**
 * @author  winstone
 */
public class NodeNameHolder {

	private static String nodeId;
	
	public static String getNodeId() {
		if(nodeId != null)return nodeId;
		try {
			nodeId = InetAddress.getLocalHost().getHostName() + "_" + RandomStringUtils.random(6, true, true).toLowerCase();
		} catch (Exception e) {
			nodeId = UUID.randomUUID().toString();
		}
		return nodeId;
	}
	
}
