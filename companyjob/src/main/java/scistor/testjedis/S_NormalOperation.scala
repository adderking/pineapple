package scistor.testjedis

import redis.clients.jedis.JedisCluster

/**
 * Created by kingcobra on 15/8/6.
 */
class S_NormalOperation {
  val jedisCluster:JedisCluster = RedisConnection.initCluster()

  def saveData {
    jedisCluster.set("testKey", "hello world")
    jedisCluster.set("testKey1", "hello world 1")
  }

  def getData {
    var value: String = jedisCluster.get("testKey")
    System.out.println(value)
    value = jedisCluster.get("testKey1")
    System.out.println(value)
  }

  def delData {
    jedisCluster.del("testKey")
    jedisCluster.del("testKey1")
  }

}
object S_NormalOperation{
  def main(args: Array[String]) {
    val n = new S_NormalOperation;
    n.saveData
    n.getData
    n.delData
  }
}
