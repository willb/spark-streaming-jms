package com.redhat.spark.streaming.jms

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import javax.jms._
import javax.naming.Context

import scala.util.Try

class UnreliableJMSReceiver(brokerURL: String, 
                            username: Option[String],
                            password: Option[String], 
                            queueName: String, 
                            selector: Option[String] = None, 
                            storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    extends BaseReceiver(brokerURL, username, password, queueName, selector, storageLevel) {

  /** Auxiliary constructor for convenience and Java interoperability */
  def this(brokerURL: String, queueName: String, username: String = null, password: String = null, selector: String = null, storageLevel: StorageLevel) {
    this(brokerURL, JavaConveniences.denull(username), JavaConveniences.denull(password), queueName, JavaConveniences.denull(selector), storageLevel)
  }

  /** Auxiliary constructor for convenience and Java interoperability */
  def this(brokerURL: String, queueName: String, username: String, password: String, selector: String) {
    this(brokerURL, username, password, queueName, selector, StorageLevel.MEMORY_AND_DISK)
  }

  /** Auxiliary constructor for convenience and Java interoperability */
  def this(brokerURL: String, queueName: String, selector: String) {
    this(brokerURL, null, null, queueName, selector, StorageLevel.MEMORY_AND_DISK)
  }
  
  var connection: Connection = null
   
  def doStart() {
    val env = new java.util.Hashtable[Object, Object]();
    env.put(Context.INITIAL_CONTEXT_FACTORY, JNDI_INITIAL_CONTEXT_FACTORY);
    env.put(JNDI_CONNECTION_FACTORY_KEY_PREFIX + JNDI_CONNECTION_FACTORY_NAME, brokerURL);
    env.put(JNDI_QUEUE_KEY_PREFIX + JNDI_QUEUE_NAME, queueName);
    val context = new javax.naming.InitialContext(env);

    val factory = context.lookup(JNDI_CONNECTION_FACTORY_NAME).asInstanceOf[ConnectionFactory];
    val queue = context.lookup(JNDI_QUEUE_NAME).asInstanceOf[Destination];

    connection = username.flatMap { u => 
      password.map { p => factory.createConnection(u, p)} 
    }.getOrElse(factory.createConnection)

    connection.setExceptionListener(new ExceptionCallback(this));

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    val messageConsumer: MessageConsumer = 
      selector.map { s => session.createConsumer(queue, s) }.getOrElse(session.createConsumer(queue))
    messageConsumer.setMessageListener(this)

    connection.start
  }
  
}

