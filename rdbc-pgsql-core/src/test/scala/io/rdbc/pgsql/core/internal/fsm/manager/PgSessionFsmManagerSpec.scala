/*
 * Copyright 2016 rdbc contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rdbc.pgsql.core.internal.fsm.manager

import io.rdbc.pgsql.core.internal.fsm.State
import io.rdbc.pgsql.core.internal.{FatalErrorHandler, RequestId}
import io.rdbc.pgsql.core.pgstruct.TxStatus
import io.rdbc.pgsql.core.{ConnId, RdbcPgsqlCoreSpec}
import org.scalamock.scalatest.MockFactory

import scala.concurrent.stm._
import scala.concurrent.ExecutionContext
import scala.util.Success

class PgSessionFsmManagerSpec
  extends RdbcPgsqlCoreSpec
    with MockFactory {

  private implicit val ec = ExecutionContext.global

  "PgSessionFsmManager" should {

    "dypa" in {
      //todo check state of freshly created manager
    }

    "dupa" in {
      //1. ifReady when ready
      //2. ifReady when busy
      //3. ifReady when session closed
      val fsm = fsmManager()
      atomic { implicit tx =>
        fsm.ready() = true
        fsm.handlingTimeout() = false
        fsm.lastRequestId() = RequestId(ConnId("test"), 1L)
        fsm.state() = State.idle(TxStatus.Idle)
      }

      val req = mockFunction[RequestId, TxStatus, String]("clientRequest")
      req.expects(RequestId(ConnId("test"), 2L), TxStatus.Idle)
        .once()
        .returning("dupa")

      fsm.ifReady(req) shouldBe Success("dupa")

      atomic { implicit tx =>
        fsm.state() shouldBe State.startingRequest
        fsm.lastRequestId() shouldBe RequestId(ConnId("test"), 2L)
        fsm.handlingTimeout() shouldBe false
        fsm.ready() shouldBe false
        fsm.readyPromise().isCompleted shouldBe false
      }
    }
  }

  private def fsmManager(): PgSessionFsmManager = {
    new PgSessionFsmManager(
      connId = ConnId("test"),
      fatalErrorHandler = new FatalErrorHandler() {
        protected[core] def handleFatalError(msg: String, cause: Throwable): Unit = ()
      }
    )
  }

}
