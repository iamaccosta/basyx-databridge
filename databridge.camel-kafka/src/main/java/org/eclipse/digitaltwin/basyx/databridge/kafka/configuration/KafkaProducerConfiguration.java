/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.databridge.kafka.configuration;

import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration;

/**
 * An implementation of kafka consumer configuration
 * @author haque
 *
 */
public class KafkaProducerConfiguration extends DataSinkConfiguration {
	private String topic;
	private int maxPublishRecords;
	private String groupId;
	private int producersCount;
	private String seekTo;
	private String serverUrl;
	private int serverPort;
	
	public KafkaProducerConfiguration() {}
	
	public KafkaProducerConfiguration(
		String uniqueId, 
		String serverUrl, 
		int serverPort, 
		String topic,
		int maxPublishRecords, 
		String groupId, 
		int producersCount, 
		String seekTo
	) {
		super(uniqueId);

		this.serverUrl = serverUrl;
		this.serverPort = serverPort;
		this.topic = topic;
		this.maxPublishRecords = maxPublishRecords;
		this.groupId = groupId;
		this.producersCount = producersCount;
		this.seekTo = seekTo;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getMaxPublishRecords() {
		return maxPublishRecords;
	}

	public void setMaxPublishRecords(int maxPublishRecords) {
		this.maxPublishRecords = maxPublishRecords;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public int getProducersCount() {
		return producersCount;
	}

	public void setProducersCount(int producersCount) {
		this.producersCount = producersCount;
	}

	public String getSeekTo() {
		return seekTo;
	}

	public void setSeekTo(String seekTo) {
		this.seekTo = seekTo;
	}

	public String getConnectionURI() {
		return 
		"kafka:" + getTopic() + "?brokers=" + getServerUrl() + ":" + getServerPort()
		+ "&maxPublishRecords=" + getMaxPublishRecords()
        + "&producersCount=" + getProducersCount()
        + "&seekTo=" + getSeekTo()
        + "&groupId="  + getGroupId();
	}
}
