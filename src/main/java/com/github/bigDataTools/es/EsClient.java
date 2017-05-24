package com.github.bigDataTools.es;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import com.google.common.collect.Lists;


/***
 *
 * @author  winstone
 *
 */

public class EsClient {

	private static String clusterName="";
	private static String inetAddress = null;
	private static Integer esHttpPort = 9200;
	private static Integer esClientPort = 9300;
	private static Client client = null;

	private EsClient() {
	}
	/**
	 * 初始化客户端
	 * @throws Exception
     */
	private static void initClient() throws Exception {
		ResourceBundle rb = ResourceBundle.getBundle("commons");
		String clusterNameStr = rb.getString("clusterName");
		String inetAddressStr = rb.getString("inetAddress");
		String esHttpPortStr =  rb.getString("httpPort");
		String esClientPortStr =  rb.getString("rpcPort");
		if (StringUtils.isNotBlank(clusterNameStr)) {
			clusterName = clusterNameStr;
		}
		if (StringUtils.isNotBlank(inetAddressStr)) {
			inetAddress = inetAddressStr;
		}
		if (StringUtils.isNotBlank(esHttpPortStr)) {
			esHttpPort = Integer.parseInt(esHttpPortStr);
		}
		if (StringUtils.isNotBlank(esClientPortStr)) {
			esClientPort = Integer.parseInt(esClientPortStr);
		}
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", clusterName).build();
		TransportClient tClient = TransportClient.builder().settings(settings)
				.build();
		List<String> addressList = Lists.newArrayList();
		if (StringUtils.isBlank(inetAddress)) {
			addressList.add("localhost:9300");
		} else {
			inetAddress = inetAddress.replaceAll(" ", "").trim();
			addressList = Arrays.asList(inetAddress.split(","));
		}
		for (int i = 0; i < addressList.size(); i++) {
			String address = addressList.get(i);
			String[] arr = address.split(";");
			Integer clientPort = esClientPort;
			if (arr.length > 1) {
				clientPort = Integer.parseInt(arr[1]);
			}
			tClient.addTransportAddress(new InetSocketTransportAddress(
					InetAddress.getByName(arr[0].trim()), clientPort));
		}
		client = tClient;
	}


	public void init() throws Exception {
		client = EsClient.getClient();
	}

	/**
	 * 获取client 客户端
	 * @return
	 * @throws Exception
     */
	public static Client getClient() throws Exception {
		if (client == null) {
			synchronized (EsClient.class) {
				if (client == null)
					initClient();
			}
		}
		return client;
	}

	/**
	 * close
	 */
	public void close() {
		if (client != null) {
			client.close();
		}
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		EsClient.clusterName = clusterName;
	}

	public String getInetAddress() {
		return inetAddress;
	}

	public void setInetAddress(String inetAddress) {
		EsClient.inetAddress = inetAddress;
	}

	public Integer getEsHttpPort() {
		return esHttpPort;
	}

	public void setEsHttpPort(Integer esHttpPort) {
		EsClient.esHttpPort = esHttpPort;
	}

	public Integer getEsClientPort() {
		return esClientPort;
	}

	public void setEsClientPort(Integer esClientPort) {
		EsClient.esClientPort = esClientPort;
	}
}
