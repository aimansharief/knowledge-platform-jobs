package org.sunbird.job.content.publish.helpers

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import java.util
import org.slf4j.LoggerFactory

class RefreshBodyHelper(config: ContentPublishConfig, httpUtil: HttpUtil, cassandraUtil: CassandraUtil) {

  private[this] val logger = LoggerFactory.getLogger(classOf[RefreshBodyHelper])

  // Parse difficultyRate string to Map (keys normalized to lowercase)
  def parseDifficultyRate(contentMeta: Map[String, AnyRef]): Map[String, Int] = {
    logger.info(s"parseDifficultyRate :: input contentMeta keys=${contentMeta.keys}")
    val difficultyRateStr = contentMeta.get("difficultyRate").map(_.toString).getOrElse("{}")
    try {
      val raw = JSONUtil.deserialize[Map[String, Int]](difficultyRateStr)
      logger.info(s"parseDifficultyRate :: parsed difficultyRate=$raw")
      raw.map { case (k, v) => (k.toLowerCase, v) }
    } catch {
      case _: Exception => Map.empty[String, Int]
    }
  }

  // Compute counts for each difficulty using multiplier
  def computeCounts(difficultyRate: Map[String, Int], difficultyMultiplier: Int): (Int, Int, Int) = {
    logger.info(s"computeCounts :: difficultyRate=$difficultyRate, multiplier=$difficultyMultiplier")
    val easy = difficultyRate.getOrElse("easy", 0) * difficultyMultiplier
    val medium = difficultyRate.getOrElse("medium", 0) * difficultyMultiplier
    val hard = difficultyRate.getOrElse("difficult", 0) * difficultyMultiplier
    logger.info(s"computeCounts :: easy=$easy, medium=$medium, hard=$hard")
    (easy, medium, hard)
  }

  // Fetch random identifiers for each difficulty level, applying observableElements filter
  def fetchRandomIdsForLevels(easyCount: Int, mediumCount: Int, hardCount: Int, observableElements: List[String]): (List[String], List[String], List[String]) = {
    logger.info(s"fetchRandomIdsForLevels :: requested counts easy=$easyCount, medium=$mediumCount, hard=$hardCount, observables=$observableElements")
    val easyIdentifiers = getIdentifiersForQlevel("EASY", observableElements)
    val mediumIdentifiers = getIdentifiersForQlevel("MEDIUM", observableElements)
    val hardIdentifiers = getIdentifiersForQlevel("DIFFICULT", observableElements)

    val randomEasyIds = pickRandomIdentifiers(easyIdentifiers, easyCount)
    val randomMediumIds = pickRandomIdentifiers(mediumIdentifiers, mediumCount)
    val randomHardIds = pickRandomIdentifiers(hardIdentifiers, hardCount)

    logger.info(s"fetchRandomIdsForLevels :: candidates sizes easy=${easyIdentifiers.size}, medium=${mediumIdentifiers.size}, hard=${hardIdentifiers.size}")
    logger.info(s"fetchRandomIdsForLevels :: selected easy=$randomEasyIds, medium=$randomMediumIds, hard=$randomHardIds")
    (randomEasyIds, randomMediumIds, randomHardIds)
  }

  // Helper: get identifiers for a qlevel with optional observableElements filter
  def getIdentifiersForQlevel(qlevel: String, observableElements: List[String]): List[String] = {
    logger.info(s"getIdentifiersForQlevel :: qlevel=$qlevel, observableElements=$observableElements")
    try {
      val obsList = new util.ArrayList[String]()
      if (observableElements != null) observableElements.foreach(obsList.add)

      val reqMap = new java.util.HashMap[String, AnyRef]() {
        put("request", new java.util.HashMap[String, AnyRef]() {
          put("filters", new java.util.HashMap[String, AnyRef]() {
            put("status", new util.ArrayList[String]() {{ add("Live") }})
            put("objectType", "AssessmentItem")
            put("qlevel", qlevel.toUpperCase)
            put("observableElement", obsList)
          })
          put("fields", new util.ArrayList[String]() {{ add("identifier"); add("qlevel") }})
          put("limit", Int.box(1000))
        })
      }

      val reqBody = JSONUtil.serialize(reqMap)
      logger.info(s"getIdentifiersForQlevel :: requestBody=$reqBody")
      val httpResponse = httpUtil.post(config.searchServiceURL, reqBody)
      logger.info(s"getIdentifiersForQlevel :: http post to ${config.searchServiceURL}, status=${httpResponse.status}")
      if (httpResponse.status == 200) {
        val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
        val result = response.getOrElse("result", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
        val items = result.getOrElse("items", List.empty[Map[String, AnyRef]]).asInstanceOf[List[Map[String, AnyRef]]]
        val count = result.getOrElse("count", Int.box(0)).asInstanceOf[Int]

        logger.info(s"getIdentifiersForQlevel :: response count=$count, itemsSize=${items.size}")
        if (count > 0) {
          items.filter(item => item.contains("identifier") && item.contains("qlevel"))
            .map(item => item.getOrElse("identifier", "").toString)
            .filter(_.nonEmpty)
        } else List.empty[String]
      } else List.empty[String]
    } catch {
      case e: Exception =>
        logger.error(s"getIdentifiersForQlevel :: exception for qlevel=$qlevel", e)
        List.empty[String]
    }
  }

  def pickRandomIdentifiers(identifiers: List[String], count: Int): List[String] = scala.util.Random.shuffle(identifiers).take(count)

  def getItemsByIdentifiers(identifiers: List[String]): List[Map[String, AnyRef]] = {
    logger.info(s"getItemsByIdentifiers :: requested identifiers=$identifiers")
    if (identifiers.isEmpty) {
      logger.info("getItemsByIdentifiers :: empty identifiers list, returning empty")
      List.empty[Map[String, AnyRef]]
    } else {
      try {
        val items = identifiers.flatMap { identifier =>
          try {
            val url = config.learningServiceURL + identifier
            logger.info(s"getItemsByIdentifiers :: making GET request to $url")
            val httpResponse = httpUtil.get(url)
            logger.info(s"getItemsByIdentifiers :: http status=${httpResponse.status}")
            if (httpResponse.status == 200) {
              val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
              val result = response.getOrElse("result", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
              val assessmentItem = result.getOrElse("assessment_item", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]
              if (assessmentItem.nonEmpty) {
                Some(assessmentItem)
              } else {
                logger.warn(s"getItemsByIdentifiers :: no assessment_item found for identifier=$identifier")
                None
              }
            } else {
              logger.error(s"getItemsByIdentifiers :: failed to fetch item for identifier=$identifier, status=${httpResponse.status}")
              None
            }
          } catch {
            case e: Exception =>
              logger.error(s"getItemsByIdentifiers :: exception fetching identifier=$identifier", e)
              None
          }
        }
        logger.info(s"getItemsByIdentifiers :: successfully fetched ${items.size} items out of ${identifiers.size} requested")
        items
      } catch {
        case e: Exception =>
          logger.error(s"getItemsByIdentifiers :: exception for ids=${identifiers}", e)
          List.empty[Map[String, AnyRef]]
      }
    }
  }

  def buildUpdatedBody(contentMeta: Map[String, AnyRef], items: List[Map[String, AnyRef]]): String = {
    val title = contentMeta.get("name").map(_.toString).getOrElse("")
    logger.info(s"buildUpdatedBody :: title=$title, itemsCount=${items.size}")
    val body = ECMLBodyBuilder.buildEcmlBodyFromItems(items, title, config)
    logger.info(s"buildUpdatedBody :: built body $body")
    body
  }

  def updateContentBody(identifier: String, ecmlBody: String): Unit = {
    logger.info(s"updateContentBody :: updating content id=$identifier")
    val updateQuery = QueryBuilder.update(config.contentKeyspaceName, config.contentTableName)
      .where(QueryBuilder.eq("content_id", identifier))
      .`with`(QueryBuilder.set("body", QueryBuilder.fcall("textAsBlob", ecmlBody)))
    cassandraUtil.upsert(updateQuery.toString)
    logger.info(s"updateContentBody :: upsert query executed for id=$identifier")
  }

  def getContentMetaData(identifier: String): Map[String, AnyRef] = {
    logger.info(s"getContentMetaData :: fetching metadata for id=$identifier")
    try {
      val response = httpUtil.get(config.contentReadURL + identifier)
      logger.info(s"getContentMetaData :: http status=${response.status}")
      val obj = JSONUtil.deserialize[Map[String, AnyRef]](response.body)
      val content = obj.get("result").flatMap(_.asInstanceOf[Map[String, AnyRef]].get("content")).map(_.asInstanceOf[Map[String, AnyRef]]).getOrElse(Map.empty)
      logger.info(s"getContentMetaData :: fetched keys=${content.keys}")
      content
    } catch {
      case e: Exception =>
        logger.error(s"getContentMetaData :: exception fetching id=$identifier", e)
        Map.empty[String, AnyRef]
    }
  }
}
