package models

import java.util.Date

/**
  * Created by chenxm on 10/4/16.
  */
case class Dataset(id: Long,
                   name: String,
                   contributor: Long, // User.id
                   introShort: String,
                   introLong: String,
                   keywords: List[String],
                   category: String,
                   source: String,
                   createdAt: Date,
                   updatedAt: Date)
