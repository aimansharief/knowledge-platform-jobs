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

class CFBatchCreateFunction(config: PostPublishProcessorConfig, httpUtil: HttpUtil)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) with BatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[CFBatchCreateFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val activityId = eData.getOrDefault("identifier", "")
    metrics.incCounter(config.CFBatchCreationCount)
    val startDate = ZonedDateTime.now(ZoneId.of(config.timezone)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    logger.info("Creating Competency Framework Batch for " + activityId + " with start date:" + startDate)
    try {
      createCFBatch(eData, startDate)(config, httpUtil)
      metrics.incCounter(config.CFBatchCreationSuccessCount)
      logger.info("Competency Framework Batch created for " + activityId)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing Competency Framework batch creation for identifier : ${activityId}.", ex)
        metrics.incCounter(config.CFBatchCreationFailedCount)
        throw ex
    }
  }

  override def metricsList(): List[String] = {
    List(config.CFBatchCreationCount, config.CFBatchCreationSuccessCount, config.CFBatchCreationFailedCount)
  }

}
