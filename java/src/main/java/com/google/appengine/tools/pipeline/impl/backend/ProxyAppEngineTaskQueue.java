// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl.backend;

import com.gigware.cloudmine.appengine.Appengine;
import com.gigware.cloudmine.appengine.model.Header;
import com.gigware.cloudmine.appengine.model.Param;
import com.gigware.cloudmine.appengine.model.RetryOptions;
import com.gigware.cloudmine.appengine.model.TaskHandle;
import com.gigware.cloudmine.appengine.model.TaskOptions;
import com.gigware.deferred.DeferredTask;
import com.gigware.deferred.DeferredTaskContext;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.ObjRefTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Encapsulates access to the App Engine Task Queue API
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class ProxyAppEngineTaskQueue implements PipelineTaskQueue {

  public static final String TASKQUEUE_PROXY_URL_PROPERTY = "cloudaware.taskqueue.proxy.url";
  private static final String TASKQUEUE_PROXY_KEY_PROPERTY = "cloudaware.taskqueue.proxy.key";
  private static final Logger logger = Logger.getLogger(ProxyAppEngineTaskQueue.class.getName());

  private final String proxyKey;
  private final Appengine appengine;

  public ProxyAppEngineTaskQueue() {
    final String proxyUrl = System.getProperty(TASKQUEUE_PROXY_URL_PROPERTY);
    final String proxyKey = System.getProperty(TASKQUEUE_PROXY_KEY_PROPERTY);
    this.proxyKey = proxyKey;
    try {
      Appengine.Builder builder = new Appengine.Builder(
              GoogleNetHttpTransport.newTrustedTransport(),
              JacksonFactory.getDefaultInstance(),
              new HttpRequestInitializer() {
                @Override
                public void initialize(final HttpRequest request) throws IOException {

                }
              }
      ).setApplicationName("appengine-pipeline");
      if (proxyUrl != null) {
        builder.setRootUrl(proxyUrl);
      }
      appengine = builder.build();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ProxyAppEngineTaskQueue(final Appengine appengine) {
    this(appengine, null);
  }

  public ProxyAppEngineTaskQueue(final Appengine appengine, final String proxyKey) {
    this.proxyKey = proxyKey;
    this.appengine = appengine;
  }

  private static void addProperties(TaskOptions taskOptions, Properties properties) {
    for (String paramName : properties.stringPropertyNames()) {
      String paramValue = properties.getProperty(paramName);
      List<Param> params = taskOptions.getParams();
      if (params == null) {
        params = Lists.newArrayList();
        taskOptions.setParams(params);
      }
      params.add(new Param().setName(paramName).setStringValue(paramValue));
    }
  }

  @Override
  public void enqueueDeferred(final String queueNameArg, final DeferredTask deferredTask) {
    String queueName = Optional.fromNullable(queueNameArg).or("default");
    TaskOptions taskOptions = new TaskOptions();
    taskOptions
            .setCountdownMillis(10000L)
            .setRetryOptions(new RetryOptions()
                    .setMinBackoffSeconds(2.).setMaxBackoffSeconds(20.)
            );
    DeferredTaskContext.enqueueDeferred(deferredTask, taskOptions, queueName);
  }

  @Override
  public void enqueue(Task task) {
    logger.finest("Enqueueing: " + task);
    TaskOptions taskOptions = toTaskOptions(task);
    try {
      Appengine.Queues.Tasks.Add add = appengine
              .queues()
              .tasks()
              .add(getQueueName(task.getQueueSettings().getOnQueue()), taskOptions);
      if (this.proxyKey != null) {
        add.setKey(this.proxyKey);
      }
      add.execute();
    } catch (IOException e) {
      if (e instanceof GoogleJsonResponseException && e.getMessage().startsWith("com.google.appengine.api.taskqueue.TaskAlreadyExistsException:")) {
        //ignore
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void enqueue(final Collection<Task> tasks) {
    addToQueue(tasks);
  }

  //VisibleForTesting
  List<TaskHandle> addToQueue(final Collection<Task> tasks) {
    List<TaskHandle> handles = new ArrayList<>();
    Map<String, List<TaskOptions>> queueNameToTaskOptions = new HashMap<>();
    for (Task task : tasks) {
      logger.finest("Enqueueing: " + task);
      String queueName = task.getQueueSettings().getOnQueue();
      queueName = queueName == null ? "default" : queueName;
      try {
        Appengine.Queues.Tasks.Add add = appengine
                .queues()
                .tasks()
                .add(
                        queueName,
                        toTaskOptions(task)
                );
        if (this.proxyKey != null) {
          add.setKey(this.proxyKey);
        }
        handles.add(add.execute());
      } catch (IOException e) {
        if (e instanceof GoogleJsonResponseException && e.getMessage().startsWith("com.google.appengine.api.taskqueue.TaskAlreadyExistsException:")) {
          throw new TaskAlreadyExistException(e);
        } else {
          throw new RuntimeException(e);
        }
      }
    }
    return handles;
  }

  private String getQueueName(final String queue) {
    return queue == null ? "default" : queue;
  }

  private TaskOptions toTaskOptions(Task task) {
    final QueueSettings queueSettings = task.getQueueSettings();

    String url = TaskHandler.handleTaskUrl();
    url += "/taskClass:" + task.getClass().getSimpleName();
    if (task instanceof ObjRefTask) {
      url += "/objRefTaskKey:" + ((ObjRefTask) task).getKey().getName();
    }
    url += "/taskName:" + task.getName();
    TaskOptions taskOptions = new TaskOptions().setUrl(url);
    if (queueSettings.getOnBackend() != null) {
      taskOptions.setHeaders(
              Lists.newArrayList(
                      new Header()
                              .setHeaderName("Host")
                              .setHeaderValues(
                                      Lists.newArrayList(queueSettings.getOnBackend())
                              )
              )
      );
    } else {
      throw new RuntimeException("Appengine pipeline removed support of Module");
    }

    Long delayInSeconds = queueSettings.getDelayInSeconds();
    if (null != delayInSeconds) {
      taskOptions.setCountdownMillis(delayInSeconds * 1000L);
      queueSettings.setDelayInSeconds(null);
    }
    if (queueSettings.getQueueRetryTaskRetryLimit() != null
            || queueSettings.getQueueRetryTaskAgeLimitSeconds() != null
            || queueSettings.getQueueRetryMinBackoffSeconds() != null
            || queueSettings.getQueueRetryMaxBackoffSeconds() != null
            || queueSettings.getQueueRetryMaxDoublings() != null) {
      RetryOptions retryOptions = new RetryOptions();
      if (queueSettings.getQueueRetryTaskRetryLimit() != null) {
        retryOptions.setTaskRetryLimit(queueSettings.getQueueRetryTaskRetryLimit().intValue());
      }
      if (queueSettings.getQueueRetryTaskAgeLimitSeconds() != null) {
        retryOptions.setTaskAgeLimitSeconds(queueSettings.getQueueRetryTaskAgeLimitSeconds());
      }
      if (queueSettings.getQueueRetryMinBackoffSeconds() != null) {
        retryOptions.setMinBackoffSeconds(queueSettings.getQueueRetryMinBackoffSeconds().doubleValue());
      }
      if (queueSettings.getQueueRetryMaxBackoffSeconds() != null) {
        retryOptions.setMaxBackoffSeconds(queueSettings.getQueueRetryMaxBackoffSeconds().doubleValue());
      }
      if (queueSettings.getQueueRetryMaxDoublings() != null) {
        retryOptions.setMaxDoublings(queueSettings.getQueueRetryMaxDoublings().intValue());
      }
      taskOptions.setRetryOptions(retryOptions);
    }
    addProperties(taskOptions, task.toProperties());
    String taskName = task.getName();
    if (null != taskName) {
      taskOptions.setTaskName(taskName);
    }
    return taskOptions;
  }
}
