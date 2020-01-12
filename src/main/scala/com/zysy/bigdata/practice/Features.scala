package com.zysy.bigdata.practice

import com.esri.core.geometry.{Geometry, GeometryEngine}
import org.json4s.{Formats, JObject, NoTypeHints}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

class Features {

}

case class FeatureCollection(
                              features: List[Feature]
                            )

case class Feature(
                    id: Int,
                    properties: Map[String, String],
                    geometry: JObject
                  ) {
  def getGeometry: Geometry = {
    GeometryEngine.geoJsonToGeometry(compact(render(geometry)), 0, Geometry.Type.Unknown).getGeometry
  }
}


case class FeatureProperties(boroughCode: Int, borough: String)

object FeatureExtraction {
  def main(args: Array[String]): Unit = {
    val json =
      """
        |{
        |	"type": "FeatureCollection",
        |	"features": [
        |    {
        |      "type": "Feature",
        |      "id": 0,
        |      "properties": {
        |        "boroughCode": 5,
        |        "borough": "Staten Island",
        |        "@id": "http:\/\/nyc.pediacities.com\/Resource\/Borough\/Staten_Island"
        |      },
        |      "geometry": {
        |        "type": "Polygon",
        |        "coordinates": [
        |          [
        |            [-74.050508064032471, 40.566422034160816],
        |            [-74.049983525625748, 40.566395924928273]
        |          ]
        |        ]
        |      }
        |    }
        |  ]
        |}
      """.stripMargin

    val collection: FeatureCollection = parseJson(json)
    print(collection)
  }

  def parseJson(json: String): FeatureCollection = {
    import org.json4s.jackson.Serialization.{read, write}


    implicit val format: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val featureCollection = read[FeatureCollection](json)
    featureCollection
  }
}