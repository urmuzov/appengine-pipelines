package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.common.base.MoreObjects;

/**
 * Queue settings implementation.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public final class QueueSettings implements Cloneable {

  private String onBackend;
  private String onModule;
  private String moduleVersion;
  private String onQueue;
  private Long queueRetryTaskRetryLimit;
  private Long queueRetryTaskAgeLimitSeconds;
  private Long queueRetryMinBackoffSeconds;
  private Long queueRetryMaxBackoffSeconds;
  private Long queueRetryMaxDoublings;
  private Long delay;

  /**
   * Merge will override any {@code null} setting with a matching setting from {@code other}.
   * Note, delay value is not being merged. moduleVersion is only copied if onModule is copied.
   */
  public QueueSettings merge(QueueSettings other) {
    if (onBackend == null && onModule == null) {
      onBackend = other.getOnBackend();
    }
    if (onModule == null && onBackend == null) {
      onModule = other.getOnModule();
      moduleVersion = other.getModuleVersion();
    }
    if (onQueue == null) {
      onQueue = other.getOnQueue();
    }
    if (queueRetryTaskRetryLimit == null) {
      queueRetryTaskRetryLimit = other.getQueueRetryTaskRetryLimit();
    }
    if (queueRetryTaskAgeLimitSeconds == null) {
      queueRetryTaskAgeLimitSeconds = other.getQueueRetryTaskAgeLimitSeconds();
    }
    if (queueRetryMinBackoffSeconds == null) {
      queueRetryMinBackoffSeconds = other.getQueueRetryMinBackoffSeconds();
    }
    if (queueRetryMaxBackoffSeconds == null) {
      queueRetryMaxBackoffSeconds = other.getQueueRetryMaxBackoffSeconds();
    }
    if (queueRetryMaxDoublings == null) {
      queueRetryMaxDoublings = other.getQueueRetryMaxDoublings();
    }
    return this;
  }

  public QueueSettings setOnBackend(String onBackend) {
    if (onBackend != null && onModule != null) {
      throw new IllegalStateException("OnModule and OnBackend cannot be combined");
    }
    this.onBackend = onBackend;
    return this;
  }

  public String getOnBackend() {
    return onBackend;
  }

  public QueueSettings setOnModule(String onModule) {
    if (onModule != null && onBackend != null) {
      throw new IllegalStateException("OnModule and OnBackend cannot be combined");
    }
    this.onModule = onModule;
    return this;
  }

  public String getOnModule() {
    return onModule;
  }

  public QueueSettings setModuleVersion(String moduleVersion) {
    this.moduleVersion = moduleVersion;
    return this;
  }

  public String getModuleVersion() {
    return moduleVersion;
  }

  public QueueSettings setOnQueue(String onQueue) {
    this.onQueue = onQueue;
    return this;
  }

  public Long getQueueRetryTaskRetryLimit() {
    return queueRetryTaskRetryLimit;
  }

  public QueueSettings setQueueRetryTaskRetryLimit(Long queueRetryTaskRetryLimit) {
    this.queueRetryTaskRetryLimit = queueRetryTaskRetryLimit;
    return this;
  }

  public Long getQueueRetryMaxBackoffSeconds() {
    return queueRetryMaxBackoffSeconds;
  }

  public QueueSettings setQueueRetryMaxBackoffSeconds(Long queueRetryMaxBackoffSeconds) {
    this.queueRetryMaxBackoffSeconds = queueRetryMaxBackoffSeconds;
    return this;
  }

  public Long getQueueRetryMaxDoublings() {
    return queueRetryMaxDoublings;
  }

  public QueueSettings setQueueRetryMaxDoublings(Long queueRetryMaxDoublings) {
    this.queueRetryMaxDoublings = queueRetryMaxDoublings;
    return this;
  }

  public Long getQueueRetryMinBackoffSeconds() {
    return queueRetryMinBackoffSeconds;
  }

  public QueueSettings setQueueRetryMinBackoffSeconds(Long queueRetryMinBackoffSeconds) {
    this.queueRetryMinBackoffSeconds = queueRetryMinBackoffSeconds;
    return this;
  }

  public Long getQueueRetryTaskAgeLimitSeconds() {
    return queueRetryTaskAgeLimitSeconds;
  }

  public QueueSettings setQueueRetryTaskAgeLimitSeconds(Long queueRetryTaskAgeLimitSeconds) {
    this.queueRetryTaskAgeLimitSeconds = queueRetryTaskAgeLimitSeconds;
    return this;
  }

  public String getOnQueue() {
    return onQueue;
  }

  public void setDelayInSeconds(Long delay) {
    this.delay = delay;
  }

  public Long getDelayInSeconds() {
    return delay;
  }

  @Override
  public QueueSettings clone() {
    try {
      return (QueueSettings) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Should never happen", e);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("onBackend", onBackend)
            .add("onModule", onModule)
            .add("moduleVersion", moduleVersion)
            .add("onQueue", onQueue)
            .add("queueRetryTaskRetryLimit", queueRetryTaskRetryLimit)
            .add("queueRetryTaskAgeLimitSeconds", queueRetryTaskAgeLimitSeconds)
            .add("queueRetryMinBackoffSeconds", queueRetryMinBackoffSeconds)
            .add("queueRetryMaxBackoffSeconds", queueRetryMaxBackoffSeconds)
            .add("queueRetryMaxDoublings", queueRetryMaxDoublings)
            .toString();
  }
}
