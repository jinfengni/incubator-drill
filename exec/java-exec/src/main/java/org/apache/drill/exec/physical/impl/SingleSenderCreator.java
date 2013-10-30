/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl;

import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;
import io.netty.buffer.ByteBuf;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitTunnel;

public class SingleSenderCreator implements RootCreator<SingleSender>{

  public RootExec getRoot(FragmentContext context, SingleSender config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    return new SingleSenderRootExec(context, children.iterator().next(), config);
  }
  
  
  private static class SingleSenderRootExec implements RootExec{
    private final MetricRegistry metrics = DrillMetrics.getInstance();
    private final String GETTER_TIMER = MetricRegistry.name(SingleSenderRootExec.class, "GetterTimer");
    private final String SETTER_TIMER = MetricRegistry.name(SingleSenderRootExec.class, "SenderTimer");
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSenderRootExec.class);

    private RecordBatch incoming;
    private BitTunnel tunnel;
    private FragmentHandle handle;
    private int recMajor;
    private FragmentContext context;
    private volatile boolean ok = true;
    
    public SingleSenderRootExec(FragmentContext context, RecordBatch batch, SingleSender config){
      this.incoming = batch;
      assert(incoming != null);
      this.handle = context.getHandle();
      this.recMajor = config.getOppositeMajorFragmentId();
      this.tunnel = context.getCommunicator().getTunnel(config.getDestination());
      this.context = context;
    }
    
    @Override
    public boolean next() {
      if(!ok){
        incoming.kill();
        
        return false;
      }
      Timer.Context getterContext = metrics.timer(GETTER_TIMER).time();
      IterOutcome out = incoming.next();
      getterContext.stop();
      Timer.Context senderContext = metrics.timer(SETTER_TIMER).time();
      logger.debug("Outcome of sender next {}", out);
      switch(out){
      case STOP:
      case NONE:
        FragmentWritableBatch b2 = new FragmentWritableBatch(true, handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), recMajor, 0, incoming.getWritableBatch());
        tunnel.sendRecordBatch(new RecordSendFailure(), context, b2);
        senderContext.stop();
        return false;

      case OK_NEW_SCHEMA:
      case OK:
        FragmentWritableBatch batch = new FragmentWritableBatch(false, handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), recMajor, 0, incoming.getWritableBatch());
        tunnel.sendRecordBatch(new RecordSendFailure(), context, batch);
        senderContext.stop();
        return true;

      case NOT_YET:
      default:
        senderContext.stop();
        throw new IllegalStateException();
      }
    }

    @Override
    public void stop() {
      ok = false;
    }
    
    
    private class RecordSendFailure extends BaseRpcOutcomeListener<Ack>{

      @Override
      public void failed(RpcException ex) {
        context.fail(ex);
        stop();
      }

      @Override
      public void success(Ack value, ByteBuf buf) {
        if(value.getOk()) return;

        logger.error("Downstream fragment was not accepted.  Stopping future sends.");
        // if we didn't get ack ok, we'll need to kill the query.
        context.fail(new RpcException("A downstream fragment batch wasn't accepted.  This fragment thus fails."));
        stop();
      }
      
    }
    
  }
  

}
