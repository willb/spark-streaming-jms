package com.redhat.spark.streaming.jms

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import javax.jms._
import javax.naming.Context

import scala.util.Try

trait JNDIConstants {
  val JNDI_INITIAL_CONTEXT_FACTORY       = "org.apache.qpid.jms.jndi.JmsInitialContextFactory";
  val JNDI_CONNECTION_FACTORY_NAME       = "JMSReceiverConnectionFactory";
  val JNDI_QUEUE_NAME                    = "JMSReceiverQueue";
  val JNDI_CONNECTION_FACTORY_KEY_PREFIX = "connectionfactory.";
  val JNDI_QUEUE_KEY_PREFIX              = "queue.";
  
}

private [jms] class ExceptionCallback[R <: Receiver[JMSEvent]](val parent: R) extends ExceptionListener {
  override def onException(exp: JMSException) {
    parent.reportError("Connection ExceptionListener fired, attempting restart.", exp)
    parent.restart("Connection ExceptionListener fired, attempting restart.")
  }
}

private [jms] object JavaConveniences {
  def denull[A <: Any](ref: A): Option[A] = if (ref == null) None else Some(ref)
}

class UnreliableJMSReceiver(val brokerURL: String, 
                            val username: Option[String],
                            val password: Option[String], 
                            val queueName: String, 
                            val selector: Option[String] = None, 
                            override val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK)
    extends Receiver[JMSEvent](storageLevel) with MessageListener with JNDIConstants {

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

  private var connection: Connection = null

  def onMessage(message: Message) {
    Try(new JMSEvent(message)).map { jmsEvent =>
      store(jmsEvent)
    }.recover( { case exp: RuntimeException => reportError("Caught exception converting JMS message to JMSEvent", exp) } )
  }
    
  def onStart() {
    Try(doStart()).recover { 
      case exp: RuntimeException => {
        reportError("Caught exception in startup", exp);
        restart("Caught exception, restarting", exp);
      }
    }// Caught exception, try a restart
  }
    
  private [this] def doStart() {
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

    def onStop {
        // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data

        Try(connection.close()).recover { case exp: RuntimeException => reportError("Caught exception stopping", exp) }
    }

    override def toString = 
      s"JMSReceiver{brokerURL=$brokerURL, username=$username, password=$password, " + 
      s"queueName=$queueName, selector=$selector)"
}

