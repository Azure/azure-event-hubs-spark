/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming.eventhubs

import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Assertions, BeforeAndAfter}
import org.scalatest.concurrent.{Eventually, Timeouts}
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.DebugFilesystem
import org.apache.spark.eventhubscommon.EventHubsConnector
import org.apache.spark.eventhubscommon.client.EventHubsOffsetTypes.EventHubsOffsetType
import org.apache.spark.eventhubscommon.progress.ProgressTrackerBase
import org.apache.spark.eventhubscommon.utils._
import org.apache.spark.sql.{Dataset, Encoder, QueryTest, Row}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.eventhubs.checkpoint.StructuredStreamingProgressTracker
import org.apache.spark.sql.test.{SharedSQLContext, TestSparkSession}
import org.apache.spark.util.{Clock, ManualClock, SystemClock, Utils}

/**
 * A framework for implementing tests for streaming queries and sources.
 *
 * A test consists of a set of steps (expressed as a `StreamAction`) that are executed in order,
 * blocking as necessary to let the stream catch up.  For example, the following adds some data to
 * a stream, blocking until it can verify that the correct values are eventually produced.
 *
 * {{{
 *  val inputData = MemoryStream[Int]
 *  val mapped = inputData.toDS().map(_ + 1)
 *
 *  testStream(mapped)(
 *    AddData(inputData, 1, 2, 3),
 *    CheckAnswer(2, 3, 4))
 * }}}
 *
 * Note that while we do sleep to allow the other thread to progress without spinning,
 * `StreamAction` checks should not depend on the amount of time spent sleeping.  Instead they
 * should check the actual progress of the stream before verifying the required test condition.
 *
 * Currently it is assumed that all streaming queries will eventually complete in 10 seconds to
 * avoid hanging forever in the case of failures. However, individual suites can change this
 * by overriding `streamingTimeout`.
 */

trait EventHubsStreamTest extends QueryTest with BeforeAndAfter
  with SharedSQLContext with Timeouts with Serializable {

  protected val tempRoot = "/tmp"

  override def afterAll(): Unit = {
    super.afterAll()
    FileSystem.get(new Configuration()).delete(new Path(s"$tempRoot/test-sql-context"), true)
  }

  override protected def createSparkSession: TestSparkSession = {
    new TestSparkSession(
      sparkConf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName).setAppName(
        s"EventHubsStreamTest_${System.currentTimeMillis()}"))
  }

  /** How long to wait for an active stream to catch up when checking a result. */
  val streamingTimeout = 60.seconds


  /** A trait for actions that can be performed while testing a streaming DataFrame. */
  // trait StreamAction

  /** A trait to mark actions that require the stream to be actively running. */
  trait StreamMustBeRunning

  /** A trait that can be extended when testing a source. */
  trait ExternalAction extends StreamAction with Serializable {
    def runAction(): Unit
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
   * This operation automatically blocks until all added data has been processed.
   */
  object CheckAnswer {

    def apply[A : Encoder](isSort: Boolean, data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        data.map(d => toExternalRow.fromRow(encoder.toRow(d))),
        lastOnly = false,
        isSorted = isSort)
    }

    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        data.map(d => toExternalRow.fromRow(encoder.toRow(d))),
        lastOnly = false,
        isSorted = false)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, false, false)

    def apply[A : Encoder](partial: Boolean, lastOnly: Boolean, rows: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        rows.map(r => toExternalRow.fromRow(encoder.toRow(r))),
        isSorted = false, lastOnly = lastOnly, ifCheckPartialResult = partial)
    }
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
   * This operation automatically blocks until all added data has been processed.
   */
  object CheckLastBatch {
    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      apply(isSorted = false, data: _*)
    }

    def apply[A: Encoder](isSorted: Boolean, data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        data.map(d => toExternalRow.fromRow(encoder.toRow(d))),
        lastOnly = true,
        isSorted = isSorted)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, lastOnly = true,
      isSorted = false)
  }

  case class CheckAnswerRows(
      expectedAnswer: Seq[Row],
      lastOnly: Boolean,
      isSorted: Boolean,
      ifCheckPartialResult: Boolean = false)
    extends StreamAction with StreamMustBeRunning {
    override def toString: String = s"$operatorName: ${expectedAnswer.mkString(",")}"
    private def operatorName = if (lastOnly) "CheckLastBatch" else "CheckAnswer"
  }

  case class UpdatePartialCheck(expectedOffset: Offset) extends StreamAction

  /** Stops the stream. It must currently be running. */
  case class StopStream(recoverStreamId: Boolean = false,
                        commitOffset: Boolean = false,
                        commitPartialOffset: Boolean = false,
                        partialType: String = "delete")
    extends StreamAction with StreamMustBeRunning

  /** Starts the stream, resuming if data has already been processed. It must not be running. */
  case class StartStream(trigger: Trigger = ProcessingTime(0),
                         triggerClock: Clock = new SystemClock,
                         additionalConfs: Map[String, String] = Map.empty)
    extends StreamAction

  /** Advance the trigger clock's time manually. */
  case class AdvanceManualClock(timeToAdd: Long) extends StreamAction

  /** Signals that a failure is expected and should not kill the test. */
  case class ExpectFailure[T <: Throwable : ClassTag]() extends StreamAction {
    val causeClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    override def toString: String = s"ExpectFailure[${causeClass.getName}]"
  }

  /** Assert that a body is true */
  class Assert(condition: => Boolean, val message: String = "") extends StreamAction {
    def run(): Unit = { Assertions.assert(condition) }
    override def toString: String = s"Assert(<condition>, $message)"
  }

  object Assert {
    def apply(condition: => Boolean, message: String = ""): Assert = new Assert(condition, message)
    def apply(message: String)(body: => Unit): Assert = new Assert( { body; true }, message)
    def apply(body: => Unit): Assert = new Assert( { body; true }, "")
  }

  /** Assert that a condition on the active query is true */
  class AssertOnQuery(val condition: StreamExecution => Boolean, val message: String)
    extends StreamAction {
    override def toString: String = s"AssertOnQuery(<condition>, $message)"
  }

  object AssertOnQuery {
    def apply(condition: StreamExecution => Boolean, message: String = ""): AssertOnQuery = {
      new AssertOnQuery(condition, message)
    }

    def apply(message: String)(condition: StreamExecution => Boolean): AssertOnQuery = {
      new AssertOnQuery(condition, message)
    }
  }

  class StreamManualClock(@volatile var currentTime: Long = 0L)
    extends ManualClock(currentTime) with Serializable {

    private var waitStartTime: Option[Long] = None

    private var stream: StreamExecution = _

    /**
     * @return `ManualClock` with initial time 0
     */
    def this() = this(0L)

    def setStream(currentStream: StreamExecution): Unit = {
      stream = currentStream
    }

    override def getTimeMillis(): Long =
      synchronized {
        currentTime
      }

    /**
     * @param timeToSet new time (in milliseconds) that the clock should represent
     */
    override def setTime(timeToSet: Long): Unit = synchronized {
      currentTime = timeToSet
      notifyAll()
    }

    /**
     * @param timeToAdd time (in milliseconds) to add to the clock's time
     */
    override def advance(timeToAdd: Long): Unit = synchronized {
      currentTime += timeToAdd
      notifyAll()
    }

    /**
     * @param targetTime block until the clock time is set or advanced to at least this time
     * @return current time reported by the clock when waiting finishes
     */
    override def waitTillTime(targetTime: Long): Long = synchronized {
      try {
        waitStartTime = Some(getTimeMillis())
        while (currentTime < targetTime && (stream == null || stream.isActive)) {
          wait(10)
        }
        getTimeMillis()
      } finally {
        waitStartTime = None
      }
    }

    def isStreamWaitingAt(time: Long): Boolean = synchronized {waitStartTime == Some(time)}
  }


  /**
   * Executes the specified actions on the given streaming DataFrame and provides helpful
   * error messages in the case of failures or incorrect answers.
   *
   * Note that if the stream is not explicitly started before an action that requires it to be
   * running then it will be automatically started before performing any other actions.
   */
  def testStream(_stream: Dataset[_],
                 outputMode: OutputMode = OutputMode.Append)(actions: StreamAction*): Unit = {

    val stream = _stream.toDF()
    val sparkSession = stream.sparkSession  // use the session in DF, not the default session
    var pos = 0
    var currentStream: StreamExecution = null
    var lastStream: StreamExecution = null
    val awaiting = new mutable.HashMap[Int, Offset]() // source index -> offset to wait for
    val partialAwaiting = new mutable.HashMap[Int, Offset]()
    var sink = new MemorySink(stream.schema, outputMode)
    val resetConfValues = mutable.Map[String, Option[String]]()

    @volatile
    var streamDeathCause: Throwable = null

    // If the test doesn't manually start the stream, we do it automatically at the beginning.
    val startedManually =
      actions.takeWhile(!_.isInstanceOf[StreamMustBeRunning]).exists(_.isInstanceOf[StartStream])
    val startedTest = if (startedManually) actions else StartStream() +: actions

    def testActions = actions.zipWithIndex.map {
      case (a, i) =>
        if ((pos == i && startedManually) || (pos == (i + 1) && !startedManually)) {
          "=> " + a.toString
        } else {
          "   " + a.toString
        }
    }.mkString("\n")

    def currentOffsets =
      if (currentStream != null) currentStream.committedOffsets.toString else "not started"

    def threadState =
      if (currentStream != null && currentStream.microBatchThread.isAlive) "alive" else "dead"

    def testState =
      s"""
         |== Progress ==
         |$testActions
         |
         |== Stream ==
         |Output Mode: $outputMode
         |Stream state: $currentOffsets
         |Thread state: $threadState
         |${if (streamDeathCause != null) stackTraceToString(streamDeathCause) else ""}
         |
         |== Sink ==
         |${sink.toDebugString}
         |
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.lastExecution else ""}
         """.stripMargin

    def verify(condition: => Boolean, message: String): Unit = {
      if (!condition) {
        failTest(message)
      }
    }

    def eventually[T](message: String)(func: => T): T = {
      try {
        Eventually.eventually(Timeout(streamingTimeout)) {
          func
        }
      } catch {
        case NonFatal(e) =>
          failTest(message, e)
      }
    }

    def findSourceIndex(plan: LogicalPlan, source: Source): Option[Int] = {
      plan
        .collect { case StreamingExecutionRelation(s, _) => s }
        .zipWithIndex
        .find(_._1 == source)
        .map(_._2)
    }

    def failTest(message: String, cause: Throwable = null) = {

      // Recursively pretty print a exception with truncated stacktrace and internal cause
      def exceptionToString(e: Throwable, prefix: String = ""): String = {
        val base = s"$prefix${e.getMessage}" +
          e.getStackTrace.take(10).mkString(s"\n$prefix", s"\n$prefix\t", "\n")
        if (e.getCause != null) {
          base + s"\n$prefix\tCaused by: " + exceptionToString(e.getCause, s"$prefix\t")
        } else {
          base
        }
      }
      val c = Option(cause).map(exceptionToString(_))
      val m = if (message != null && message.nonEmpty) Some(message) else None
      fail(
        s"""
           |${(m ++ c).mkString(": ")}
           |$testState
         """.stripMargin)
    }

    def searchCurrentSource(): EventHubsSource = {
      val sources = currentStream.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[EventHubsSource] =>
          source.asInstanceOf[EventHubsSource]
      }

      if (sources.isEmpty) {
        throw new Exception("Could not find EventHubs source in the StreamExecution" +
          " logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception("Could not select the EventHubs source in the StreamExecution " +
          "logical plan as there" +
          "are multiple EventHubs sources:\n\t" + sources.mkString("\n\t"))
      }
      sources.head
    }

    def createBrokenProgressFile(
        progressTracker: ProgressTrackerBase[_ <: EventHubsConnector],
        timestamp: Long,
        brokenType: String): Unit = {
      val progressDir = progressTracker.progressDirectoryPath.toString
      val metadataDir = progressTracker.progressMetadataDirectoryPath.toString
      val progressFilePath = new Path(s"$progressDir/progress-$timestamp")
      val metadataFilePath = new Path(s"$metadataDir/$timestamp")
      val fs = progressFilePath.getFileSystem(new Configuration())
      if (brokenType == "delete") {
        fs.delete(progressFilePath, true)
        fs.delete(metadataFilePath, true)
      } else if (brokenType == "deletemetadata") {
        fs.delete(metadataFilePath, true)
      } else if (brokenType == "partial" ) {
        fs.delete(progressFilePath, true)
        fs.delete(metadataFilePath, true)
        val fsos = fs.create(progressFilePath)
        fsos.writeBytes(s"$timestamp ns1_eh1_23 eh1 1 499 499")
        fsos.close()
      } else if (brokenType == "nometadata") {
        fs.delete(new Path(metadataDir), true)
      } else {
        throw new Exception(s"unrecognizable broken type $brokenType")
      }
    }

    val metadataRoot = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    var manualClockExpectedTime = -1L
    try {
      startedTest.foreach { action =>
        logInfo(s"Processing test stream action: $action")
        action match {
          case StartStream(trigger, triggerClock, additionalConfs) =>
            verify(currentStream == null, "stream already running")
            verify(triggerClock.isInstanceOf[SystemClock]
              || triggerClock.isInstanceOf[StreamManualClock],
              "Use either SystemClock or StreamManualClock to start the stream")
            if (triggerClock.isInstanceOf[StreamManualClock]) {
              manualClockExpectedTime = triggerClock.asInstanceOf[StreamManualClock].getTimeMillis()
            }

            additionalConfs.foreach(pair => {
              val value =
                if (sparkSession.conf.contains(pair._1)) {
                  Some(sparkSession.conf.get(pair._1))
                } else None
              resetConfValues(pair._1) = value
              sparkSession.conf.set(pair._1, pair._2)
            })

            lastStream = currentStream
            val createQueryMethod = sparkSession.streams.getClass.getDeclaredMethods.filter(m =>
              m.getName == "createQuery").head
            createQueryMethod.setAccessible(true)
            val checkpointLocation = additionalConfs.getOrElse[String](
              "eventhubs.test.checkpointLocation",
              metadataRoot)
            if (additionalConfs.contains("eventhubs.test.newSink") &&
              additionalConfs("eventhubs.test.newSink").toBoolean) {
              sink = new MemorySink(stream.schema, outputMode)
            }
            currentStream = createQueryMethod.invoke(
              sparkSession.streams,
              None,
              Some(checkpointLocation),
              stream,
              sink,
              outputMode,
              Boolean.box(false), // useTempCheckpointLocation
              Boolean.box(true), // recoverFromCheckpointLocation
              trigger,
              triggerClock).asInstanceOf[StreamExecution]

            triggerClock match {
              case smc: StreamManualClock =>
                smc.setStream(currentStream)
              case _ =>
            }

            val activeQueriesField = sparkSession.streams.getClass.getDeclaredFields.filter(f =>
              f.getName == "org$apache$spark$sql$streaming$StreamingQueryManager$$activeQueries").
              head
            activeQueriesField.setAccessible(true)
            val activeQueries = activeQueriesField.get(sparkSession.streams).
              asInstanceOf[mutable.HashMap[UUID, StreamingQuery]]
            activeQueries += currentStream.id -> currentStream

            val eventHubsSource = searchCurrentSource()
            val eventHubs = EventHubsTestUtilities.getOrSimulateEventHubs(null)
            eventHubsSource.setEventHubClient(new SimulatedEventHubsRestClient(eventHubs))
            eventHubsSource.setEventHubsReceiver(
              (eventHubsParameters: Map[String, String], partitionId: Int,
               startOffset: Long, offsetType: EventHubsOffsetType, _: Int) =>
                new TestEventHubsReceiver(eventHubsParameters, eventHubs, partitionId, startOffset,
                  offsetType)
            )
            currentStream.start()

          case AdvanceManualClock(timeToAdd) =>
            verify(currentStream != null,
              "can not advance manual clock when a stream is not running")
            verify(currentStream.triggerClock.isInstanceOf[StreamManualClock],
              s"can not advance clock of type ${currentStream.triggerClock.getClass}")
            val clock = currentStream.triggerClock.asInstanceOf[StreamManualClock]
            assert(manualClockExpectedTime >= 0)

            // Make sure we don't advance ManualClock too early. See SPARK-16002.
            eventually("StreamManualClock has not yet entered the waiting state") {
              assert(clock.isStreamWaitingAt(manualClockExpectedTime))
            }

            clock.advance(timeToAdd)
            manualClockExpectedTime += timeToAdd
            verify(clock.getTimeMillis() === manualClockExpectedTime,
              s"Unexpected clock time after updating: " +
                s"expecting $manualClockExpectedTime, current ${clock.getTimeMillis()}")

          case StopStream(recoverStreamId: Boolean, commitOffset: Boolean,
            commitPartialOffset: Boolean, partialType: String) =>
            verify(currentStream != null, "can not stop a stream that is not running")
            require(!(commitOffset && commitPartialOffset),
              "cannot set both of commitOffset and commitPartialOffset as true")
            if (recoverStreamId) {
              EventHubsSource.streamIdGenerator.decrementAndGet()
            }
            try failAfter(streamingTimeout) {
              currentStream.stop()
              if (commitOffset) {
                val source = searchCurrentSource()
                source.collectFinishedBatchOffsetsAndCommit(
                  source.committedOffsetsAndSeqNums.batchId + 1)
              }
              if (commitPartialOffset) {
                val source = searchCurrentSource()
                val progressTracker = StructuredStreamingProgressTracker.getInstance(source.uid)
                source.collectFinishedBatchOffsetsAndCommit(
                  source.committedOffsetsAndSeqNums.batchId + 1)
                createBrokenProgressFile(progressTracker,
                  source.committedOffsetsAndSeqNums.batchId, partialType)
              }
              verify(!currentStream.microBatchThread.isAlive,
                s"microbatch thread not stopped")
              verify(!currentStream.isActive,
                "query.isActive() is false even after stopping")
              verify(currentStream.exception.isEmpty,
                s"query.exception() is not empty after clean stop: " +
                  currentStream.exception.map(_.toString()).getOrElse(""))
            } catch {
              case _: InterruptedException =>
              case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                failTest("Timed out while stopping and waiting for microbatchthread to terminate.")
              case t: Throwable =>
                failTest("Error while stopping stream", t)
            } finally {
              lastStream = currentStream
              currentStream = null
            }

          case ef: ExpectFailure[_] =>
            verify(currentStream != null, "can not expect failure when stream is not running")
            try failAfter(streamingTimeout) {
              val thrownException = intercept[StreamingQueryException] {
                currentStream.awaitTermination()
              }
              eventually("microbatch thread not stopped after termination with failure") {
                assert(!currentStream.microBatchThread.isAlive)
              }
              verify(currentStream.exception === Some(thrownException),
                s"incorrect exception returned by query.exception()")

              val exception = currentStream.exception.get
              verify(exception.cause.getClass === ef.causeClass,
                "incorrect cause in exception returned by query.exception()\n" +
                  s"\tExpected: ${ef.causeClass}\n\tReturned: ${exception.cause.getClass}")
            } catch {
              case _: InterruptedException =>
              case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                failTest("Timed out while waiting for failure")
              case t: Throwable =>
                failTest("Error while checking stream failure", t)
            } finally {
              lastStream = currentStream
              currentStream = null
              streamDeathCause = null
            }
          case a: AssertOnQuery =>
            verify(currentStream != null || lastStream != null,
              "cannot assert when not stream has been started")
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            verify(a.condition(streamToAssert), s"Assert on query failed: ${a.message}")
          case a: Assert =>
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            verify({ a.run(); true }, s"Assert failed: ${a.message}")
          case UpdatePartialCheck(expectedOffset: Offset) =>
            val sources = currentStream.logicalPlan.collect {
              case StreamingExecutionRelation(source, _) if source.isInstanceOf[EventHubsSource] =>
                source.asInstanceOf[EventHubsSource]
            }.head
            val sourceIndex = findSourceIndex(currentStream.logicalPlan, sources)
            partialAwaiting.put(sourceIndex.get, expectedOffset)
          case a: EventHubsAddData =>
            try {
              // Add data and get the source where it was added, and the expected offset of the
              // added data.
              val queryToUse = Option(currentStream).orElse(Option(lastStream))
              val (source, offset) = a.addData(queryToUse)
              // Try to find the index of the source to which data was added. Either get the index
              // from the current active query or the original input logical plan.
              val sourceIndex =
              queryToUse.flatMap { query =>
                findSourceIndex(query.logicalPlan, source)
              }.orElse {
                findSourceIndex(stream.logicalPlan, source)
              }.getOrElse {
                throw new IllegalArgumentException(
                  "Could find index of the source to which data was added")
              }
              // Store the expected offset of added data to wait for it later
              awaiting.put(sourceIndex, offset)
            } catch {
              case NonFatal(e) =>
                failTest("Error adding data", e)
            }

          case e: ExternalAction =>
            e.runAction()

          case CheckAnswerRows(expectedAnswer, lastOnly, isSorted, partial) =>
            verify(currentStream != null, "stream not running")
            // Get the map of source index to the current source objects
            val indexToSource = currentStream
              .logicalPlan
              .collect { case StreamingExecutionRelation(s, _) => s }
              .zipWithIndex
              .map(_.swap)
              .toMap

            // Block until all data added has been processed for all the source
            {if (!partial) awaiting else partialAwaiting}.foreach { case (sourceIndex, offset) =>
              try {
                failAfter(streamingTimeout) {
                  currentStream.awaitOffset(indexToSource(sourceIndex), offset)
                }
              } catch {
                case e: Exception =>
                  e.printStackTrace()
                  throw e
              }
            }
            val sparkAnswer = try if (lastOnly) sink.latestBatchData else sink.allData catch {
              case e: Exception =>
                failTest("Exception while getting data from sink", e)
            }

            QueryTest.sameRows(expectedAnswer, sparkAnswer, isSorted).foreach {
              error => failTest(error)
            }
        }
        pos += 1
      }
    } catch {
      case _: InterruptedException if streamDeathCause != null =>
        failTest("Stream Thread Died")
      case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
        failTest("Timed out waiting for stream")
    } finally {
      try {
        failAfter(streamingTimeout) {
          if (currentStream != null && currentStream.microBatchThread.isAlive) {
            currentStream.stop()
          }
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          failTest(s"cannot stop currentStream at ${currentStream.triggerClock.getTimeMillis()}")
      }

      // Rollback prev configuration values
      resetConfValues.foreach {
        case (key, Some(value)) => sparkSession.conf.set(key, value)
        case (key, None) => sparkSession.conf.unset(key)
      }
    }
  }

  /**
   * Creates a stress test that randomly starts/stops/adds data/checks the result.
   *
   * @param ds         a dataframe that executes + 1 on a stream of integers, returning the result
   * @param addData    an add data action that adds the given numbers to the stream, encoding them
   *                   as needed
   * @param iterations the iteration number
   */
  def runStressTest(ds: Dataset[Int],
                    addData: Seq[Int] => StreamAction,
                    iterations: Int = 100): Unit = {
    runStressTest(ds, Seq.empty, (data, running) => addData(data), iterations)
  }

  /**
   * Creates a stress test that randomly starts/stops/adds data/checks the result.
   *
   * @param ds             a dataframe that executes + 1 on a stream of integers,
   *                       returning the result
   * @param prepareActions actions need to run before starting the stress test.
   * @param addData        an add data action that adds the given numbers to the stream,
   *                       encoding them
   *                       as needed
   * @param iterations     the iteration number
   */
  def runStressTest(ds: Dataset[Int],
                    prepareActions: Seq[StreamAction],
                    addData: (Seq[Int], Boolean) => StreamAction,
                    iterations: Int): Unit = {
    implicit val intEncoder = ExpressionEncoder[Int]()
    var dataPos = 0
    var running = true
    val actions = new ArrayBuffer[StreamAction]()
    actions ++= prepareActions

    def addCheck() = { actions += CheckAnswer(1 to dataPos: _*) }

    def addRandomData() = {
      val numItems = Random.nextInt(10)
      val data = dataPos until (dataPos + numItems)
      dataPos += numItems
      actions += addData(data, running)
    }

    (1 to iterations).foreach { i =>
      val rand = Random.nextDouble()
      if(!running) {
        rand match {
          case r if r < 0.7 => // AddData
            addRandomData()

          case _ => // StartStream
            actions += StartStream()
            running = true
        }
      } else {
        rand match {
          case r if r < 0.1 =>
            addCheck()

          case r if r < 0.7 => // AddData
            addRandomData()

          case _ => // StopStream
            addCheck()
            actions += StopStream()
            running = false
        }
      }
    }
    if(!running) { actions += StartStream() }
    addCheck()
    testStream(ds)(actions: _*)
  }

  object AwaitTerminationTester {

    trait ExpectedBehavior

    /** Expect awaitTermination to not be blocked */
    case object ExpectNotBlocked extends ExpectedBehavior

    /** Expect awaitTermination to get blocked */
    case object ExpectBlocked extends ExpectedBehavior

    /** Expect awaitTermination to throw an exception */
    case class ExpectException[E <: Exception]()(implicit val t: ClassTag[E])
      extends ExpectedBehavior

    private val DEFAULT_TEST_TIMEOUT = 1.second

    def test(expectedBehavior: ExpectedBehavior, awaitTermFunc: () => Unit,
             testTimeout: Span = DEFAULT_TEST_TIMEOUT): Unit = {
      expectedBehavior match {
        case ExpectNotBlocked =>
          withClue("Got blocked when expected non-blocking.") {
            failAfter(testTimeout) {
              awaitTermFunc()
            }
          }
        case ExpectBlocked =>
          withClue("Was not blocked when expected.") {
            intercept[TestFailedDueToTimeoutException] {
              failAfter(testTimeout) {
                awaitTermFunc()
              }
            }
          }
        case e: ExpectException[_] =>
          val thrownException =
            withClue(s"Did not throw ${e.t.runtimeClass.getSimpleName} when expected.") {
              intercept[StreamingQueryException] {
                failAfter(testTimeout) {
                  awaitTermFunc()
                }
              }
            }
          assert(thrownException.cause.getClass === e.t.runtimeClass,
            "exception of incorrect type was throw")
      }
    }
  }
}
