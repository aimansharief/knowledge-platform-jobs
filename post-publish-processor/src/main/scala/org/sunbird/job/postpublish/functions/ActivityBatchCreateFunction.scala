package org.sunbird.job.postpublish.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.postpublish.helpers.BatchCreation
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util.HttpUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

class ActivityBatchCreateFunction(config: PostPublishProcessorConfig, httpUtil: HttpUtil)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) with BatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[ActivityBatchCreateFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val activityId = eData.getOrDefault("identifier", "")
    metrics.incCounter(config.activityBatchCreationCount)
    val startDate = ZonedDateTime.now(ZoneId.of(config.timezone)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    logger.info("Creating Activity Batch for " + activityId + " with start date:" + startDate)
    try {
      createActivityBatch(eData, startDate)(config, httpUtil)
      metrics.incCounter(config.activityBatchCreationSuccessCount)
      logger.info("Activity Batch created for " + activityId)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing Activity batch creation for identifier : ${activityId}.", ex)
        metrics.incCounter(config.activityBatchCreationFailedCount)
        throw ex
    }
  }

  override def metricsList(): List[String] = {
    List(config.activityBatchCreationCount, config.activityBatchCreationSuccessCount, config.activityBatchCreationFailedCount)
  }

}
