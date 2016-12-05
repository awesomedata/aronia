package models

case class User(account: String,
                firstName: String,
                lastName: String,
                email: String,
                passwd: String,
                active: Boolean,
                created: Long,
                updated: Long)

object JsonFormats {
  import play.api.libs.json.Json

  // Generates Writes and Reads for Feed and User thanks to Json Macros
  implicit val userFormat = Json.format[User]
}