package org.sunbird.job.content.function

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.content.publish.domain.Event
import org.sunbird.job.content.publish.helpers.RefreshBodyHelper
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}

class RefreshBodyFunction(config: ContentPublishConfig) extends BaseProcessFunction[Event, String](config) {

  @transient private var httpUtil: HttpUtil = _
  private[this] val logger = LoggerFactory.getLogger(classOf[RefreshBodyFunction])
  @transient private var cassandraUtil: CassandraUtil = _

  // helper instance will be created in open() after non-serializable resources are initialized
  @transient private var helper: RefreshBodyHelper = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // initialize non-serializable resources here so they are created on TaskManagers
    httpUtil = new HttpUtil
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    helper = new RefreshBodyHelper(config, httpUtil, cassandraUtil)
  }

  override def close(): Unit = {
    try {
      if (cassandraUtil != null) cassandraUtil.close()
    } catch {
      case _: Exception => // ignore
    }
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.skippedEventCount, config.totalEventsCount)
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info(s"RefreshBodyFunction :: Received event for identifier: ${event.identifier}, action=${event.action}")
    try {
      val contentMeta: Map[String, AnyRef] = helper.getContentMetaData(event.identifier)
      logger.debug(s"processElement :: contentMeta keys=${contentMeta.keys}")

      val difficultyRate = helper.parseDifficultyRate(contentMeta)
      logger.debug(s"processElement :: difficultyRate=$difficultyRate")

      val (easyCount, mediumCount, hardCount) = helper.computeCounts(difficultyRate, config.difficultyMultiplier)
      logger.info(s"processElement :: computed counts easy=$easyCount, medium=$mediumCount, hard=$hardCount")

      val observableElements = contentMeta.getOrElse("se_observableElements", List.empty[String]).asInstanceOf[List[String]]
      logger.info(s"processElement :: observableElements=$observableElements")

      val (randomEasyIds, randomMediumIds, randomHardIds) = helper.fetchRandomIdsForLevels(easyCount, mediumCount, hardCount, observableElements)

      logger.info(s"RefreshBodyFunction :: EASY identifiers: $randomEasyIds")
      logger.info(s"RefreshBodyFunction :: MEDIUM identifiers: $randomMediumIds")
      logger.info(s"RefreshBodyFunction :: HARD identifiers: $randomHardIds")

      val allIds = (randomEasyIds ++ randomMediumIds ++ randomHardIds).distinct
      logger.info(s"processElement :: total selected ids count=${allIds.size}")

      val items = if (allIds.isEmpty) List.empty[Map[String, AnyRef]] else helper.getItemsByIdentifiers(allIds)
      logger.info(s"processElement :: fetched items count=${items.size}")

      val updatedBody = helper.buildUpdatedBody(contentMeta, items)
      logger.info(s"RefreshBodyFunction :: built updated body for identifier=${event.identifier}")
      helper.updateContentBody(event.identifier,updatedBody)
    } catch {
      case e: Exception =>
        logger.error("RefreshBodyFunction :: processElement :: Exception", e)
        // route to failed event out tag
        metrics.incCounter(config.skippedEventCount)
        throw e
    }
  }
}
