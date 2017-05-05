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

import com.gigware.deferred.DeferredTask;
import com.gigware.deferred.DeferredTaskContext;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudtasks.v2beta2.CloudTasks;
import com.google.api.services.cloudtasks.v2beta2.CloudTasksScopes;
import com.google.api.services.cloudtasks.v2beta2.model.AppEngineRouting;
import com.google.api.services.cloudtasks.v2beta2.model.AppEngineTaskTarget;
import com.google.api.services.cloudtasks.v2beta2.model.CreateTaskRequest;
import com.google.api.services.cloudtasks.v2beta2.model.RetryConfig;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.ObjRefTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
 * Encapsulates access to the App Engine Task Queue API
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class CloudTaskQueue implements PipelineTaskQueue {

  public static final String CLOUDTASKS_API_ROOT_URL_PROPERTY = "cloudtasks.api.root.url";
  private static final String CLOUDTASKS_API_KEY_PROPERTY = "cloudtasks.api.key";
  private static final String CLOUDTASKS_API_DEFAULT_PARENT = "cloudtasks.api.default.parent";
  private static final Logger logger = Logger.getLogger(CloudTaskQueue.class.getName());

  private final String apiKey;
  private final CloudTasks cloudTask;

  public CloudTaskQueue() {
    final String rootUrl = System.getProperty(CLOUDTASKS_API_ROOT_URL_PROPERTY);
    final String apiKey = System.getProperty(CLOUDTASKS_API_KEY_PROPERTY);
    this.apiKey = apiKey;
    try {
      CloudTasks.Builder builder = new CloudTasks.Builder(
          GoogleNetHttpTransport.newTrustedTransport(),
          JacksonFactory.getDefaultInstance(),
          GoogleCredential.getApplicationDefault().createScoped(CloudTasksScopes.all())
      ).setApplicationName("appengine-pipeline");
      if (rootUrl != null) {
        builder.setRootUrl(rootUrl);
      }
      cloudTask = builder.build();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public CloudTaskQueue(final CloudTasks cloudTask) {
    this(cloudTask, null);
  }

  public CloudTaskQueue(final CloudTasks cloudTask, final String apiKey) {
    this.apiKey = apiKey;
    this.cloudTask = cloudTask;
  }


  @Override
  public void enqueueDeferred(final String queueNameArg, final DeferredTask deferredTask) {
    String queueName = Optional.fromNullable(queueNameArg).or("default");
    com.google.api.services.cloudtasks.v2beta2.model.Task task = new com.google.api.services.cloudtasks.v2beta2.model.Task();
    task.setAppEngineTaskTarget(new AppEngineTaskTarget());
    task.setScheduleTime(getScheduleTime(10));
    task.getAppEngineTaskTarget().setRetryConfig(
        new RetryConfig()
            .setMinBackoff("2s").setMaxBackoff("20s")
    ).setAppEngineRouting(
        new AppEngineRouting()
            .setService(getCurrentService())
            .setVersion(getCurrentVersion())
    );
    DeferredTaskContext.enqueueDeferred(deferredTask, task, queueName);
  }

  @Override
  public void enqueue(Task task) {
    logger.finest("Enqueueing: " + task);
    com.google.api.services.cloudtasks.v2beta2.model.Task taskOptions = toTaskOptions(task);
    try {
      cloudTask
          .projects()
          .locations()
          .queues()
          .tasks()
          .create(
              getQueueName(task.getQueueSettings().getOnQueue()),
              new CreateTaskRequest().setTask(taskOptions)
          ).setKey(this.apiKey).execute();
    } catch (IOException e) {
      if (e instanceof GoogleJsonResponseException && ((GoogleJsonResponseException) e).getStatusCode() == 409) {
        //ignore TaskAlreadyExist
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
  List<com.google.api.services.cloudtasks.v2beta2.model.Task> addToQueue(final Collection<Task> tasks) {
    List<com.google.api.services.cloudtasks.v2beta2.model.Task> handles = new ArrayList<>();
    for (Task task : tasks) {
      logger.finest("Enqueueing: " + task);
      String queueName = task.getQueueSettings().getOnQueue();
      queueName = queueName == null ? "default" : queueName;
      try {
        final CloudTasks.Projects.Locations.Queues.Tasks.Create create = cloudTask
            .projects()
            .locations()
            .queues()
            .tasks()
            .create(
                getQueueName(queueName),
                new CreateTaskRequest().setTask(toTaskOptions(task))
            ).setKey(this.apiKey);
        handles.add(create.execute());
      } catch (IOException e) {
        if (e instanceof GoogleJsonResponseException && ((GoogleJsonResponseException) e).getStatusCode() == 409) {
          throw new TaskAlreadyExistException(e);
        } else {
          throw new RuntimeException(e);
        }
      }
    }
    return handles;
  }

  private String getQueueName(final String queue) {
    String queueName;
    if (queue == null || !queue.startsWith("projects/")) {
      queueName = System.getProperty(CLOUDTASKS_API_DEFAULT_PARENT) + "/queues/" + (queue == null ? "default" : queue);
    } else {
      queueName = queue;
    }
    return queueName;
  }

  private com.google.api.services.cloudtasks.v2beta2.model.Task toTaskOptions(Task task) {
    final QueueSettings queueSettings = task.getQueueSettings();


    final StringBuilder relativeUrl = new StringBuilder(TaskHandler.handleTaskUrl());

    relativeUrl.append("/taskClass:").append(task.getClass().getSimpleName());
    if (task instanceof ObjRefTask) {
      relativeUrl.append("/objRefTaskKey:").append(((ObjRefTask) task).getKey().getName());
    }
    relativeUrl.append("/taskName:").append(task.getName());
    relativeUrl.append("?");
    final Properties props = task.toProperties();
    for (final String paramName : props.stringPropertyNames()) {
      relativeUrl.append("&").append(paramName).append("=").append(props.getProperty(paramName));
    }

    final com.google.api.services.cloudtasks.v2beta2.model.Task taskOptions = new com.google.api.services.cloudtasks.v2beta2.model.Task();
    taskOptions.setAppEngineTaskTarget(
        new AppEngineTaskTarget()
            .setHeaders(Maps.<String, String>newHashMap())
            .setRelativeUrl(relativeUrl.toString())
    );

    if (queueSettings.getOnBackend() != null) {
      taskOptions.getAppEngineTaskTarget().getHeaders().put("Host", queueSettings.getOnBackend());
    }
    AppEngineRouting appEngineRouting = taskOptions.getAppEngineTaskTarget().getAppEngineRouting();
    if (appEngineRouting == null) {
      appEngineRouting = new AppEngineRouting();
      taskOptions.getAppEngineTaskTarget().setAppEngineRouting(appEngineRouting);
    }
    String onService = queueSettings.getOnService();
    if (onService == null) {
      onService = getCurrentService();
    }
    appEngineRouting.setService(onService);
    String onVersion = queueSettings.getOnVersion();
    if (onVersion == null) {
      onVersion = getCurrentVersion();
    }
    appEngineRouting.setVersion(onVersion);

    Long delayInSeconds = queueSettings.getDelayInSeconds();
    if (null != delayInSeconds) {
      taskOptions.setScheduleTime(getScheduleTime(delayInSeconds.intValue()));
    }
    if (queueSettings.getQueueRetryTaskRetryLimit() != null
        || queueSettings.getQueueRetryTaskAgeLimitSeconds() != null
        || queueSettings.getQueueRetryMinBackoffSeconds() != null
        || queueSettings.getQueueRetryMaxBackoffSeconds() != null
        || queueSettings.getQueueRetryMaxDoublings() != null) {
      RetryConfig retryConfig = new RetryConfig();
      if (queueSettings.getQueueRetryTaskRetryLimit() != null) {
        retryConfig.setMaxAttempts(queueSettings.getQueueRetryTaskRetryLimit().intValue());
      }
      if (queueSettings.getQueueRetryTaskAgeLimitSeconds() != null) {
        retryConfig.setTaskAgeLimit(queueSettings.getQueueRetryTaskAgeLimitSeconds().toString());
      }
      if (queueSettings.getQueueRetryMinBackoffSeconds() != null) {
        retryConfig.setMinBackoff(queueSettings.getQueueRetryMinBackoffSeconds().toString());
      }
      if (queueSettings.getQueueRetryMaxBackoffSeconds() != null) {
        retryConfig.setMaxBackoff(queueSettings.getQueueRetryMaxBackoffSeconds().toString());
      }
      if (queueSettings.getQueueRetryMaxDoublings() != null) {
        retryConfig.setMaxDoublings(queueSettings.getQueueRetryMaxDoublings().intValue());
      }
      taskOptions.getAppEngineTaskTarget().setRetryConfig(retryConfig);
    }

    String taskName = task.getName();
    if (null != taskName && null != task.getQueueSettings().getOnQueue()) {
      String fullName = getQueueName(task.getQueueSettings().getOnQueue()) + "/tasks/" + taskName;
      taskOptions.setName(fullName);
    }
    return taskOptions;
  }

  /**
   * Add seconds to Now() and serialize it to ISO 8601
   * @param seconds
   * @return date in ISO 8601
   */
  private String getScheduleTime(final int seconds) {
    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
    df.setTimeZone(tz);
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.SECOND, seconds);
    return df.format(calendar.getTime());
  }

  private static String getCurrentService() {
    String service = System.getenv("GAE_SERVICE");
    return service == null ? System.getProperty("GAE_SERVICE") : service;
  }

  private static String getCurrentVersion() {
    String version = System.getenv("GAE_VERSION");
    return version == null ? System.getProperty("GAE_VERSION") : version;
  }
}
