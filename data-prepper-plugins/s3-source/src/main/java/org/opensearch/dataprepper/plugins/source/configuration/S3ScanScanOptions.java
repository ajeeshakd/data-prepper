/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.plugins.source.CustomLocalDateTimeDeserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Class consists the scan options list bucket configuration properties.
 */
public class S3ScanScanOptions {
    @JsonDeserialize(using = DurationDeserializer.class)
    @JsonProperty("range")
    private Duration range;

    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    @JsonProperty("start_time")
    private LocalDateTime startTime;

    @JsonDeserialize(using = CustomLocalDateTimeDeserializer.class)
    @JsonProperty("end_time")
    private LocalDateTime endTime;

    @JsonProperty("buckets")
    private List<S3ScanBucketOptions> buckets;

    @JsonProperty("scheduling")
    private S3ScanSchedulingOptions schedulingOptions = new S3ScanSchedulingOptions();

    @AssertTrue(message = "At most two options from start_time, end_time and range can be specified at the same time")
    public boolean hasValidTimeOptions() {
        return Stream.of(startTime, endTime, range).filter(Objects::nonNull).count() < 3;
    }

    public Duration getRange() {
        return range;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public LocalDateTime getEndTime() { return endTime; }

    public List<S3ScanBucketOptions> getBuckets() {
        return buckets;
    }

    public S3ScanSchedulingOptions getSchedulingOptions() {
        return schedulingOptions;
    }
}
