package models

case class BehaviorData(uid: String,
                        time: String,
                        phoneType: String,
                        clickCount: Int)

case class BehaviorInfo(uid: Long,
                        time: String,
                        phoneType: String,
                        clickCount: Int)

case class UserData(uid: String,
                    sex: String,
                    age: Int,
                    created_time: String)
case class UserInfo(uid: Long,
                    sex: String,
                    age: Int,
                    created_time: String)
case class UserDetail(uid: Long,
                    sex: String,
                    age: Int,
                    created_time: Long)

case class NumData(id: Int)

case class Diamond(ts: Long,
                   srcuid: Long,
                   isAdd: Int,
                   sourceType: Int,
                   itemType: Int,
                   value: Long
                  )
case class DiamondStr(ts: String,
                      srcuid: Long,
                      isAdd: Int,
                      sourceType: Int,
                      itemType: Int,
                      value: Long
                     )