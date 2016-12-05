package com.aronia.core.spiders

import java.sql.SQLException

import com.typesafe.scalalogging.LazyLogging
import com.aronia.core.AroniaUtils._
import com.aronia.core.{AroniaConfig, AdsContext}
import us.codecraft.webmagic._
import us.codecraft.webmagic.handler.RequestMatcher.MatchOther
import us.codecraft.webmagic.handler.{CompositePageProcessor, CompositePipeline, PatternProcessor, RequestMatcher}

import scala.collection.JavaConverters._

/**
  * Created by chenxm on 16-7-20.
  */
class GithubStargazers extends LazyLogging {

  private val githubRepos = "https://api.github.com/repos"
  private val githubUsers = "https://api.github.com/users"
  private var context: AdsContext = _
  private var config: AroniaConfig = _
  private var clientId: String = _
  private var clientSecret: String = _
  private var numPerPage: Int = -1

  /** Parse repo profile, e.g. https://api.github.com/repos/caesar0301/awesome-public-datasets */
  private val repoProcessor: PatternProcessor = new PatternProcessor(githubRepos + "/[\\w\\-]+/[\\w\\-]+(\\?.*)?") {
    override def processPage(page: Page): MatchOther = {
      val stargazers = try {
        Option(page.getJson.jsonPath("$.stargazers_count").get).getOrElse("-1").toInt
      } catch {
        case e: Exception => -1
      }
      // Retrieve stargazers with each request for one page
      List(stargazers).foreach { source =>
        if (source > 0) {
          val requests: Seq[String] = for {
            pageIndex <- 1 to 2 //(source / numPerPage + 1)
          } yield {
            assembleUrl(trimUrlParams(page.getUrl.get) + "/stargazers", Map(
              ("page", pageIndex.toString),
              ("per_page", numPerPage.toString),
              ("client_id", clientId),
              ("client_secret", clientSecret)
            )).get
          }
          page.addTargetRequests(requests.toList.asJava)
        }
      }
      RequestMatcher.MatchOther.YES
    }

    override def processResult(resultItems: ResultItems, task: Task): MatchOther = {
      RequestMatcher.MatchOther.YES
    }
  }

  /**
    * Parse the list of stargazers of one repo, e.g.
    * https://api.github.com/repos/caesar0301/awesome-public-datasets/stargazers?page=1&per_page=50
    */
  private val stargazersProcessor: PatternProcessor = new PatternProcessor(githubRepos + "/[\\w\\-]+/[\\w\\-]+/stargazers(\\?.*)?") {
    override def processPage(page: Page): MatchOther = {
      val requests = for {
        user <- page.getJson.jsonPath("$..login").all.toArray
      } yield {
        assembleUrl(githubUsers + "/" + user, Map(
          ("client_id", clientId),
          ("client_secret", clientSecret)
        )).get
      }
      page.addTargetRequests(requests.toList.asJava)
      RequestMatcher.MatchOther.YES
    }

    override def processResult(resultItems: ResultItems, task: Task): MatchOther = {
      RequestMatcher.MatchOther.YES
    }
  }

  /** Parse detailed profile of one user, e.g. https://api.github.com/users/caesar0301 */
  private val userProcessor: PatternProcessor = new PatternProcessor(githubUsers + "/[\\w\\-]+(\\?.*)?") {

    val FIELD_LOGIN = "login"
    val FIELD_ID = "id"
    val FIELD_AVATAR_URL = "avatar_url"
    val FIELD_GRAVATAR_ID = "gravatar_id"
    val FIELD_NAME = "name"
    val FIELD_COMPANY = "company"
    val FIELD_BLOG = "blog"
    val FIELD_LOCATION = "location"
    val FIELD_EMAIL = "email"
    val FIELD_HIREABLE = "hireable"
    val FIELD_BIO = "bio"
    val FIELD_PUBLIC_REPOS = "public_repos"
    val FIELD_PUBLIC_GISTS = "public_gists"
    val FIELD_FOLLOWERS = "followers"
    val FIELD_FOLLOWING = "following"
    val FIELD_CREATED_AT = "created_at"
    val FIELD_UPDATED_AT = "updated_at"

    override def processPage(page: Page): MatchOther = {
      val user = page.getJson
      page.putField(FIELD_LOGIN, getJsonField("$.login", user).getOrElse(""))
      page.putField(FIELD_ID, getJsonField("$.id", user).getOrElse("-1").toInt)
      page.putField(FIELD_AVATAR_URL, getJsonField("$.avatar_url", user).getOrElse(""))
      page.putField(FIELD_GRAVATAR_ID, getJsonField("$.gravatar_id", user).getOrElse(""))
      page.putField(FIELD_NAME, getJsonField("$.name", user).getOrElse(""))
      page.putField(FIELD_COMPANY, getJsonField("$.company", user).getOrElse(""))
      page.putField(FIELD_BLOG, getJsonField("$.blog", user).getOrElse(""))
      page.putField(FIELD_LOCATION, getJsonField("$.location", user).getOrElse(""))
      page.putField(FIELD_EMAIL, getJsonField("$.email", user).getOrElse(""))
      page.putField(FIELD_HIREABLE, getJsonField("$.hireable", user).getOrElse("false").toBoolean)
      page.putField(FIELD_BIO, getJsonField("$.bio", user).getOrElse(""))
      page.putField(FIELD_PUBLIC_REPOS, getJsonField("$.public_repos", user).getOrElse("0").toInt)
      page.putField(FIELD_PUBLIC_GISTS, getJsonField("$.public_gists", user).getOrElse("0").toInt)
      page.putField(FIELD_FOLLOWERS, getJsonField("$.followers", user).getOrElse("0").toInt)
      page.putField(FIELD_FOLLOWING, getJsonField("$.following", user).getOrElse("0").toInt)
      page.putField(FIELD_CREATED_AT, getJsonField("$.created_at", user).getOrElse(""))
      page.putField(FIELD_UPDATED_AT, getJsonField("$.updated_at", user).getOrElse(""))
      RequestMatcher.MatchOther.YES
    }

    override def processResult(resultItems: ResultItems, task: Task): MatchOther = {
      val VALUE_LOGIN: String = resultItems.get(FIELD_LOGIN)
      val VALUE_ID: Int = resultItems.get(FIELD_ID)
      val VALUE_AVATAR_URL: String  = resultItems.get(FIELD_AVATAR_URL)
      val VALUE_GRAVATAR_ID: String  = resultItems.get(FIELD_GRAVATAR_ID)
      val VALUE_NAME: String  = resultItems.get(FIELD_NAME)
      val VALUE_COMPANY: String  = resultItems.get(FIELD_COMPANY)
      val VALUE_BLOG: String  = resultItems.get(FIELD_BLOG)
      val VALUE_LOCATION: String  = resultItems.get(FIELD_LOCATION)
      val VALUE_EMAIL: String  = resultItems.get(FIELD_EMAIL)
      val VALUE_HIREABLE: Boolean = resultItems.get(FIELD_HIREABLE)
      val VALUE_BIO: String  = resultItems.get(FIELD_BIO)
      val VALUE_PUBLIC_REPOS: Int = resultItems.get(FIELD_PUBLIC_REPOS)
      val VALUE_PUBLIC_GISTS: Int = resultItems.get(FIELD_PUBLIC_GISTS)
      val VALUE_FOLLOWERS: Int = resultItems.get(FIELD_FOLLOWERS)
      val VALUE_FOLLOWING: Int = resultItems.get(FIELD_FOLLOWING)
      val VALUE_CREATED_AT: String  = resultItems.get(FIELD_CREATED_AT)
      val VALUE_UPDATED_AT: String  = resultItems.get(FIELD_UPDATED_AT)

      // Save to mysqlBackend
      val tbName = "stargazers"
      val dbName = config.get(config.MYSQL_DATABASE, "awesome")
      val query = s"""INSERT INTO $dbName.$tbName (""" +
        s"""$FIELD_LOGIN, $FIELD_ID, $FIELD_AVATAR_URL, $FIELD_GRAVATAR_ID, $FIELD_NAME, $FIELD_COMPANY, $FIELD_BLOG, $FIELD_LOCATION, $FIELD_EMAIL, $FIELD_HIREABLE, $FIELD_BIO, $FIELD_PUBLIC_REPOS, $FIELD_PUBLIC_GISTS, $FIELD_FOLLOWERS, $FIELD_FOLLOWING, $FIELD_CREATED_AT, $FIELD_UPDATED_AT""" +
        s") VALUES (" +
        s""""$VALUE_LOGIN", $VALUE_ID, "$VALUE_AVATAR_URL", "$VALUE_GRAVATAR_ID", "$VALUE_NAME", "$VALUE_COMPANY", "$VALUE_BLOG", "$VALUE_LOCATION", "$VALUE_EMAIL", $VALUE_HIREABLE, "$VALUE_BIO", $VALUE_PUBLIC_REPOS, $VALUE_PUBLIC_GISTS, $VALUE_FOLLOWERS, $VALUE_FOLLOWING, "$VALUE_CREATED_AT", "$VALUE_UPDATED_AT"""" +
        s") ON DUPLICATE KEY UPDATE " +
        s"""$FIELD_AVATAR_URL = "$VALUE_AVATAR_URL", $FIELD_GRAVATAR_ID = "$VALUE_GRAVATAR_ID", $FIELD_NAME = "$VALUE_NAME", $FIELD_COMPANY = "$VALUE_COMPANY", $FIELD_BLOG = "$VALUE_BLOG", $FIELD_LOCATION = "$VALUE_LOCATION", $FIELD_EMAIL = "$VALUE_EMAIL", $FIELD_HIREABLE = $VALUE_HIREABLE, $FIELD_BIO = "$VALUE_BIO", $FIELD_PUBLIC_REPOS=$VALUE_PUBLIC_REPOS, $FIELD_PUBLIC_GISTS=$VALUE_PUBLIC_GISTS, $FIELD_FOLLOWERS=$VALUE_FOLLOWERS, $FIELD_FOLLOWING=$VALUE_FOLLOWING, $FIELD_UPDATED_AT="$VALUE_UPDATED_AT""""
      try{
        val statement = context.mysqlBackend.createStatement()
        statement.executeUpdate(query)
      } catch {
        case e: SQLException => logger.error(s"Failed to insert stargazer info:\n$query")
      }
      RequestMatcher.MatchOther.YES
    }
  }

  /** Create a spider to parse Github repo's stargazers */
  def createSpider(context: AdsContext,
                   repos: Array[String],
                   threadNum: Int,
                   sleepTime: Int = 1,
                   retryTimes: Int = 3): Spider = {
    setup(context)
    assert(repos.map(isLegalGithubRepo).toList.forall(i => i))

    val startUrls = repos.map{ repo =>
      assembleUrl(githubRepos + "/" + repo,
        Map(("client_id", clientId),
          ("client_secret", clientSecret))).get
    }
    val site = Site.me.setDomain("api.github.com")
      .setRetryTimes(retryTimes)
      .setSleepTime(sleepTime)
    val pageProcessor = new CompositePageProcessor(site)
    val pipeline = new CompositePipeline

    pageProcessor.setSubPageProcessors(
      repoProcessor,
      stargazersProcessor,
      userProcessor)
    pipeline.setSubPipeline(
      repoProcessor,
      stargazersProcessor,
      userProcessor)

    Spider.create(pageProcessor).startUrls(startUrls.toList.asJava)
      .thread(threadNum)
      .addPipeline(pipeline)
  }

  /** Initialize spider's configurations */
  def setup(context: AdsContext): Unit = {
    this.context = context
    this.config = context.config
    this.clientId = config.get(config.GITHUB_CLIENT_KEY, "unknown_id")
    this.clientSecret = config.get(config.GITHUB_CLIENT_SCR, "unknown_secret")
    this.numPerPage = config.getInt(config.GITHUB_CLIENT_PER_PAGE, 100)
  }
}
