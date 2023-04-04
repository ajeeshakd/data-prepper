/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.configuration;

import static org.junit.Assert.assertEquals;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class ConsumerGroupConfigTest {

	@Mock
	ConsumerGroupConfig consumerGroupConfig;

	@Mock
	Properties properties;

	@BeforeEach
	void setUp() throws IOException {
		consumerGroupConfig = new ConsumerGroupConfig();
		FileReader reader = new FileReader("src\\test\\resources\\pipelines.properties");
		properties = new Properties();
		properties.load(reader);

		consumerGroupConfig.setAutoCommit(properties.getProperty("autocommit"));
		consumerGroupConfig.setGroupId(properties.getProperty("group_id"));
		consumerGroupConfig.setGroupName(properties.getProperty("group_name"));
		consumerGroupConfig.setAutoCommitInterval(Duration.parse(properties.getProperty("autocommit_interval")));
		consumerGroupConfig.setSessionTimeOut(Duration.parse(properties.getProperty("session_timeout")));
		consumerGroupConfig.setMaxRetryAttempts(Integer.parseInt(properties.getProperty("max_retry_attempts")));
		consumerGroupConfig.setAutoOffsetReset(properties.getProperty("auto_offset_reset"));

		consumerGroupConfig.setMaxRetryAttempts(Integer.parseInt(properties.getProperty("max_retry_attempts")));
		consumerGroupConfig.setAutoOffsetReset(properties.getProperty("auto_offset_reset"));
		consumerGroupConfig.setWorkers(Integer.parseInt(properties.getProperty("workers")));
		consumerGroupConfig
				.setConsumerMaxPollRecords(Long.parseLong(properties.getProperty("consumer_max_poll_records")));
		consumerGroupConfig.setFetchMaxBytes(Long.parseLong(properties.getProperty("fetch_max_bytes")));
		consumerGroupConfig.setFetchMaxWait(Long.parseLong(properties.getProperty("fetch_max_wait")));
		consumerGroupConfig.setFetchMinBytes(Long.parseLong(properties.getProperty("fetch_min_bytes")));
		consumerGroupConfig.setThreadWaitingTime(Duration.parse(properties.getProperty("thread_waiting_time")));
		consumerGroupConfig.setMaxRecordFetchTime(Duration.parse(properties.getProperty("max_record_fetch_time")));
		consumerGroupConfig.setBufferDefaultTimeout(Duration.parse(properties.getProperty("buffer_default_timeout")));

	}

	@Test
	void testConfigValues() {
		assertEquals(consumerGroupConfig.getGroupId(), properties.getProperty("group_id"));
		assertEquals(consumerGroupConfig.getGroupName(), properties.getProperty("group_name"));
		assertEquals(consumerGroupConfig.getAutoCommit(), properties.getProperty("autocommit"));
		assertEquals(consumerGroupConfig.getAutoCommitInterval(),
				Duration.parse(properties.getProperty("autocommit_interval")));
		assertEquals(consumerGroupConfig.getSessionTimeOut(),
				Duration.parse(properties.getProperty("session_timeout")));
		assertEquals(consumerGroupConfig.getMaxRetryAttempts().toString(),
				properties.getProperty("max_retry_attempts"));
		assertEquals(consumerGroupConfig.getAutoCommitInterval(),
				Duration.parse(properties.getProperty("autocommit_interval")));

		assertEquals(consumerGroupConfig.getConsumerMaxPollRecords().toString(),
				properties.getProperty("consumer_max_poll_records"));
		assertEquals(consumerGroupConfig.getFetchMaxBytes().toString(), properties.getProperty("fetch_max_bytes"));
		assertEquals(consumerGroupConfig.getFetchMaxWait().toString(), properties.getProperty("fetch_max_wait"));
		assertEquals(consumerGroupConfig.getFetchMinBytes().toString(), properties.getProperty("fetch_min_bytes"));

		assertEquals(consumerGroupConfig.getMaxRetryAttempts().toString(),
				properties.getProperty("max_retry_attempts"));
		assertEquals(consumerGroupConfig.getAutoOffsetReset(), properties.getProperty("auto_offset_reset"));
		assertEquals(consumerGroupConfig.getWorkers().toString(), properties.getProperty("workers"));
		assertEquals(consumerGroupConfig.getThreadWaitingTime(),
				Duration.parse(properties.getProperty("thread_waiting_time")));
		assertEquals(consumerGroupConfig.getMaxRecordFetchTime(),
				Duration.parse(properties.getProperty("max_record_fetch_time")));
		assertEquals(consumerGroupConfig.getBufferDefaultTimeout(),
				Duration.parse(properties.getProperty("buffer_default_timeout")));

	}

}
