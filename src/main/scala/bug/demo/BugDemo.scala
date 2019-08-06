package bug.demo

import java.util.Properties

import kafka.server.KafkaConfig
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Random

object BugDemo extends EmbeddedKafka {
  val producerProps = Map(
    "bootstrap.servers" -> "localhost:5007",
    "batch.size" -> "1000000",
    "compression.type" -> "zstd",
    "linger.ms" -> "100",
    "max.in.flight.requests.per.connection" -> "5",
    "max.request.size" -> "10000000",
    "retries" -> "30",
    "retry.backoff.ms" -> "100"
  )

  val javaProps: Properties = {
    val props = new Properties()
    props.putAll(producerProps.asInstanceOf[Map[String, AnyRef]].asJava)
    props
  }

  def main(args: Array[String]): Unit = {
    implicit val embeddedConfig = EmbeddedKafkaConfig(kafkaPort = 5007, zooKeeperPort = 5006, customBrokerProperties = Map(KafkaConfig.MessageMaxBytesProp -> "500000"))
    EmbeddedKafka.start()
    createCustomTopic("maxwell.transactions", partitions = 1)

    val producer = new KafkaProducer[String, String](javaProps, new StringSerializer(), new StringSerializer())

    val ids = (1 to 50).toList
    val messages = ids.map(_ => Random.alphanumeric.take(200000).mkString)

    val future = Future.traverse(messages)(publish(producer)).map(_ => println("all published"))

    Await.result(future, 1.minute)
  }

  def publish(producer: KafkaProducer[String, String])(message: String): Future[RecordMetadata] = {
    val pr = new ProducerRecord("maxwell.transactions", "key", message)

    ProducerUtil.send(producer, pr)
  }
}

object PromiseCallbackAdaptor {
  /**
    * Completes a Scala Promise when the callback is received.
    */
  def apply[T](promise: Promise[T])(result: T, error: Throwable): Unit = {
    if (error != null) {
      promise.failure(error)
    }
    else {
      promise.success(result)
    }
  }
}


object ProducerUtil {

  // send wrapper returning a Scala future
  def send[K, V](producer: Producer[K, V], record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    producer.send(record, PromiseCallbackAdaptor(promise))
    promise.future
  }
}
