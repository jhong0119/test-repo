def getPartsUseTime300X(): (String, String, Map[String, String]) = {
  val filePath = "C:\\FREX300XLog\\FREX300X_CumLog.csv"
  var source: Option[Source] = None
  val resultMap = scala.collection.mutable.Map.empty[String, String]  // 중간에 소모품 정보 찾는 방식에 따라 mutable 처리

  try {
    source = Some(Source.fromFile(filePath)(Codec.UTF8)) // UTF-8 인코딩 적용
    // 테이블 형태를 유지하면서 CSV 파일 READ
    val lines = source.get.getLines().map(_.split(",").map(_.replaceAll("\"", "").trim)).toArray
    source.get.close()

    // CSV 헤더가 있는 줄의 인덱스를 설정
    val headerIndex = 3

    // 헤더를 읽고 따옴표 제거, TRIM 처리
    val headers = lines(headerIndex)

    // 필요한 컬럼의 위치를 찾는다
    val modulePartsIndex = headers.indexOf("Module/Parts")
    val processedTimePresentIndex = headers.indexOf("Processed Time Present")
    val processedWaferPresentIndex = headers.indexOf("Processed Wafer Present")

    // 필요한 컬럼 자체가 없으면 "NotFound"를 반환
    if (modulePartsIndex == -1 || processedTimePresentIndex == -1 || processedWaferPresentIndex == -1) {
      return ("NotFound", "NotFound", Map("NotFound" -> "NotFound"))
    }

    // 추출할 소모품 LIST 정의
    val polisherParts = Set(
      "Polisher A/Table (Polishing)", "Polisher A/Retainer Ring", "Polisher A/Dresser",
      "Polisher B/Table (Polishing)", "Polisher B/Retainer Ring", "Polisher B/Dresser",
      "Polisher C/Table (Polishing)", "Polisher C/Retainer Ring", "Polisher C/Dresser",
      "Polisher D/Table (Polishing)", "Polisher D/Retainer Ring", "Polisher D/Dresser"
    )

    val cleanerParts = Set(
      "Cleaner 1A/Upper Roll", "Cleaner 1A/Lower Roll", "Cleaner 1A/Roller (Roller)",
      "Cleaner 1B/Upper Roll", "Cleaner 1B/Lower Roll", "Cleaner 1B/Roller (Roller)",
      "Cleaner 2A/Upper Roll", "Cleaner 2A/Lower Roll", "Cleaner 2A/Roller (Roller)",
      "Cleaner 2B/Upper Roll", "Cleaner 2B/Lower Roll", "Cleaner 2B/Roller (Roller)"
    )

    // 헤더 아래부터 데이터가 시작되므로 해당 줄부터 READ
    val dataLines = lines.drop(headerIndex + 1)
    logger.info(s"dataLines : ${dataLines.map(_.mkString("|"))}")

    // "시간:분" 형식을 초 단위로 변환하는 함수 (Polisher 소모품에만 사용)
    def convertToSeconds(timeStr: String): String = {
      val cleanTime = timeStr.replaceAll("\\s", "")
      val parts = cleanTime.split(":")
      if (parts.length == 2 && parts(0).forall(_.isDigit) && parts(1).forall(_.isDigit)) {
        val hour = parts(0).toInt
        val minutes = parts(1).toInt
        ((hour * 60 + minutes) * 60).toString
      } else {
        "InvalidFormat"
      }
    }

    // Polisher와 Cleaner 목록을 합쳐서 모든 대상 소모품 목록을 만든다
    // Polisher Parts 정합성 안맞아 Clearner 만 추출
    val allTargets = cleanerParts
    //val allTargets = polisherParts ++ cleanerParts

    // 모든 소모품에 대해 데이터를 찾고 처리
    allTargets.foreach { target =>
      // 해당 소모품에 대한 ROW를 찾음 (첫 번째 컬럼과 정확히 일치하는 행을 찾음)
      val targetRow = dataLines.find(row => row.nonEmpty && row(0) == target)
      logger.info(s"targetRow : ${targetRow.map(_.mkString("|")).getOrElse("NOT FOUND")}")

      // 데이터를 찾았으면 공백/따옴표 정리, 찾지 못하면 빈 배열을 사용
      val columns = targetRow.getOrElse(Array.empty)

      if (polisherParts.contains(target)) {
        // Polisher 소모품일 경우 Processed Time Present 값을 찾는다 (못찾으면 NotFound)
        val value = if (columns.isDefinedAt(processedTimePresentIndex) && columns(processedTimePresentIndex).nonEmpty)
          convertToSeconds(columns(processedTimePresentIndex))
        else "NotFound"
        resultMap(target) = value
      } else if (cleanerParts.contains(target)) {
        // Cleaner 소모품일 경우 Processed Wafer Present 값을 찾는다 (못찾으면 NotFound)
        val value = if (columns.isDefinedAt(processedWaferPresentIndex) && columns(processedWaferPresentIndex).nonEmpty)
          columns(processedWaferPresentIndex)
        else "NotFound"
        resultMap(target) = value
      }
    }

    // ✅ RV 송신 PARTS LIST 양식에 맞게 KEY 값 변경
    var rvSendMap = scala.collection.mutable.LinkedHashMap.empty[String, String]
    //rvSendMap += ("P1" -> resultMap("Polisher A/Table (Polishing)"))
    //rvSendMap += ("P2" -> resultMap("Polisher B/Table (Polishing)"))
    //rvSendMap += ("P3" -> resultMap("Polisher C/Table (Polishing)"))
    //rvSendMap += ("P4" -> resultMap("Polisher D/Table (Polishing)"))
    //rvSendMap += ("CON1" -> resultMap("Polisher A/Dresser"))
    //rvSendMap += ("CON2" -> resultMap("Polisher B/Dresser"))
    //rvSendMap += ("CON3" -> resultMap("Polisher C/Dresser"))
    //rvSendMap += ("CON4" -> resultMap("Polisher D/Dresser"))
    //rvSendMap += ("HD1" -> resultMap("Polisher A/Retainer Ring"))
    //rvSendMap += ("HD2" -> resultMap("Polisher B/Retainer Ring"))
    //rvSendMap += ("HD3" -> resultMap("Polisher C/Retainer Ring"))
    //rvSendMap += ("HD4" -> resultMap("Polisher D/Retainer Ring"))
    rvSendMap += ("BR1" -> resultMap("Cleaner 1A/Upper Roll"))
    rvSendMap += ("BR2" -> resultMap("Cleaner 1B/Upper Roll"))
    rvSendMap += ("BR3" -> resultMap("Cleaner 2A/Upper Roll"))
    rvSendMap += ("BR4" -> resultMap("Cleaner 2B/Upper Roll"))
    rvSendMap += ("BR5" -> resultMap("Cleaner 1A/Lower Roll"))
    rvSendMap += ("BR6" -> resultMap("Cleaner 1B/Lower Roll"))
    rvSendMap += ("BR7" -> resultMap("Cleaner 2A/Lower Roll"))
    rvSendMap += ("BR8" -> resultMap("Cleaner 2B/Lower Roll"))
    rvSendMap += ("BR9" -> resultMap("Cleaner 1A/Roller (Roller)"))
    rvSendMap += ("BR10" -> resultMap("Cleaner 1B/Roller (Roller)"))
    rvSendMap += ("BR11" -> resultMap("Cleaner 2A/Roller (Roller)"))
    rvSendMap += ("BR12" -> resultMap("Cleaner 2B/Roller (Roller)"))

    // Value 값이 숫자인 소모품 LIST만 필터
    val filteredKeys = rvSendMap.filter { case (_, value) => value.forall(_.isDigit) }.keys.toList
    val rvSendPartsList = filteredKeys.mkString(",")
    val rvSendPartsUseTime = filteredKeys.map(rvSendMap).mkString(",")

    return (rvSendPartsList, rvSendPartsUseTime, resultMap.toMap)

  } catch {
    case e: Throwable =>
      return ("NotFound", "NotFound", resultMap.toMap)
  } finally {
    source.foreach(_.close())
  }
}
