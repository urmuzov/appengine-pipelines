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

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.pipeline.demo.UserGuideExamples.ComplexJob;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the sample code in the User Guide
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class UserGuideTest extends BaseEnvTest {

  private transient LocalServiceTestHelper helper;

  @Test
  public void testComplexJob() throws Exception {
    doComplexJobTest(3, 7, 11);
    doComplexJobTest(-5, 71, 6);
  }

  private void doComplexJobTest(int x, int y, int z) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new ComplexJob(), x, y, z);
    JobInfo jobInfo = service.getJobInfo(pipelineId);
    JobInfo.State state = jobInfo.getJobState();
    if (JobInfo.State.COMPLETED_SUCCESSFULLY == state) {
      System.out.println("The output is " + jobInfo.getOutput());
    }
    int output = (Integer) waitForJobToComplete(pipelineId);
    Assert.assertEquals(((x - y) * (x - z)) - 2, output);
  }

  @SuppressWarnings("unchecked")
  private <E> E waitForJobToComplete(String pipelineId) throws Exception {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    while (true) {
      Thread.sleep(2000);
      JobInfo jobInfo = service.getJobInfo(pipelineId);
      switch (jobInfo.getJobState()) {
        case COMPLETED_SUCCESSFULLY:
          return (E) jobInfo.getOutput();
        case RUNNING:
          break;
        case WAITING_TO_RETRY:
          break;
        case STOPPED_BY_ERROR:
          throw new RuntimeException("Job stopped " + jobInfo.getError());
        case STOPPED_BY_REQUEST:
          throw new RuntimeException("Job stopped by request.");
        case CANCELED_BY_REQUEST:
          throw new RuntimeException("Job cancelled by request.");
        default:
          throw new RuntimeException("Unknown Job state: " + jobInfo.getJobState());
      }
    }
  }
}
