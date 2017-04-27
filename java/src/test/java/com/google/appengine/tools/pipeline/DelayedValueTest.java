// Copyright 2013 Google Inc.
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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Test error handling through handleException.
 *
 * @author maximf@google.com (Maxim Fateev)
 */
public class DelayedValueTest extends PipelineTest {

  private static final int EXPECTED_RESULT = 5;
  private static final long DELAY_SECONDS = 10;
  private static AtomicLong duration1 = new AtomicLong();
  private static AtomicLong duration2 = new AtomicLong();
  private PipelineService service = PipelineServiceFactory.newPipelineService();

  @Test
  public void testDelayedValue() throws Exception {
    String pipelineId = service.startNewPipeline(new TestDelayedValueJob());
    Integer five = waitForJobToComplete(pipelineId);
    Assert.assertEquals(EXPECTED_RESULT, five.intValue());
    Assert.assertEquals("TestDelayedValueJob.run DelayedJob.run", trace());
    Assert.assertTrue(duration2.get() - duration1.get() >= DELAY_SECONDS * 1000);
  }

  @SuppressWarnings("serial")
  static class DelayedJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() throws Exception {
      trace("DelayedJob.run");
      duration2.set(System.currentTimeMillis());
      return immediate(EXPECTED_RESULT);
    }
  }

  @SuppressWarnings("serial")
  static class TestDelayedValueJob extends Job0<Integer> {

    @Override
    public Value<Integer> run() {
      trace("TestDelayedValueJob.run");
      duration1.set(System.currentTimeMillis());
      Value<Void> delayedValue = newDelayedValue(DELAY_SECONDS);
      return futureCall(new DelayedJob(), waitFor(delayedValue));
    }
  }
}
