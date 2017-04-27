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

package com.google.appengine.tools.pipeline;

import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.apphosting.api.ApiProxy;

import java.util.Map;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class PipelineTest extends BaseEnvTest {

  private static StringBuffer traceBuffer;
  protected ApiProxy.Environment apiProxyEnvironment;

  protected static void trace(String what) {
    if (traceBuffer.length() > 0) {
      traceBuffer.append(' ');
    }
    traceBuffer.append(what);
  }

  protected static String trace() {
    return traceBuffer.toString();
  }

  /**
   * Whether this test will succeed even if jobs remain unapplied indefinitely.
   * <p>
   * NOTE: This may be called from the constructor, i.e., before the object is
   * fully initialized.
   */
  protected boolean isHrdSafe() {
    return true;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    traceBuffer = new StringBuffer();
    apiProxyEnvironment = ApiProxy.getCurrentEnvironment();
  }

  @Override
  public void tearDown() throws Exception {

    super.tearDown();
  }

  protected void waitUntilTaskQueueIsEmpty() throws InterruptedException {
    boolean hasMoreTasks = true;
    while (hasMoreTasks) {
      Map<String, QueueStateInfo> taskInfoMap = taskQueue.getQueueStateInfo();
      hasMoreTasks = false;
      for (QueueStateInfo taskQueueInfo : taskInfoMap.values()) {
        if (taskQueueInfo.getCountTasks() > 0) {
          hasMoreTasks = true;
          break;
        }
      }
      if (hasMoreTasks) {
        Thread.sleep(100);
      }
    }
  }

  protected JobInfo waitUntilJobComplete(String pipelineId) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    while (true) {
      Thread.sleep(2000);
      JobInfo jobInfo = service.getJobInfo(pipelineId);
      switch (jobInfo.getJobState()) {
        case RUNNING:
        case WAITING_TO_RETRY:
          break;
        default:
          return jobInfo;
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected <T> T waitForJobToComplete(String pipelineId) throws Exception {
    JobInfo jobInfo = waitUntilJobComplete(pipelineId);
    switch (jobInfo.getJobState()) {
      case COMPLETED_SUCCESSFULLY:
        return (T) jobInfo.getOutput();
      case STOPPED_BY_ERROR:
        throw new RuntimeException("Job stopped " + jobInfo.getError());
      case STOPPED_BY_REQUEST:
        throw new RuntimeException("Job stopped by request.");
      case CANCELED_BY_REQUEST:
        throw new RuntimeException("Job was canceled by request.");
      default:
        throw new RuntimeException("Unexpected job state: " + jobInfo.getJobState());
    }
  }
}
