package com.mate1.tungsten

import java.util.{UUID, Date}
import java.util.ArrayList
import java.util.Iterator
import java.util.logging.Logger
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import com.continuent.tungsten.replicator.applier.RawApplier
import com.continuent.tungsten.replicator.ReplicatorException
import com.continuent.tungsten.replicator.database.Table
import com.continuent.tungsten.replicator.dbms.DBMSData
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment
import com.continuent.tungsten.replicator.dbms.OneRowChange
import com.continuent.tungsten.replicator.dbms.RowChangeData
import com.continuent.tungsten.replicator.dbms.RowIdData
import com.continuent.tungsten.replicator.dbms.StatementData
import com.continuent.tungsten.replicator.dbms.OneRowChange._
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
import com.continuent.tungsten.replicator.event.DBMSEvent
import com.continuent.tungsten.replicator.event.ReplDBMSHeader
import com.continuent.tungsten.replicator.plugin.PluginContext
import kafka.message.ByteBufferMessageSet
import kafka.message.Message
import kafka.message.NoCompressionCodec
import kafka.consumer.ConsumerConfig
import kafka.consumer.Consumer._
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.ProducerData
import kafka.utils.Utils
import com.typesafe.config.ConfigFactory
import java.io.File
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable
import com.continuent.tungsten.replicator.database.Database
import java.sql.Statement
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime
import com.continuent.tungsten.replicator.database.DatabaseFactory
import com.continuent.tungsten.replicator.applier.ApplierException
import com.continuent.tungsten.replicator.event.ReplDBMSEvent
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory
import scala.collection.mutable.HashSet

object KafkaApplerTest {
	val appConf = ConfigFactory.parseFile(new File("conf/kafka-applier.conf"))
	val conf = appConf.getConfig("kafka-applier")
	
	val topicConf = conf.getConfig("topics")
	
	topicConf.entrySet().filter(_.getKey().contains(".")).foreach(e => {
		val Array(db, table) = e.getKey().split("\\.", 1)
		val cfg = e.getValue()
	})
}

object PubSubTest extends App {
	val s1 = new StringSubscriber("sub-1")
	val s2 = new StringSubscriber("sub-2")
	val s3 = new StringSubscriber("sub-3")
	
	val p = new StringPubSub()
	p.addTopic("db1.table1")
	p.addTopic("db2.table2")
	
	p.subscribe(s1, "db1.table1")
	
	p.subscribe(s2, "db1.table1")
	p.subscribe(s2, "db2.table2")
	
	p.subscribe(s3, "db1.*")
	
	p.publish("db1.table1", new StringMessage("message 1"))
	p.publish("db2.table2", new StringMessage("message 2"))
}

trait Subscriber {
	def receive(topic:String, message:Message)
}

class Topic(name:String) {
	val subscribers = new HashSet[Subscriber]()
	
	def subscribe(sub:Subscriber):Boolean = {
		subscribers += sub
		true
	}
	
	def publish(message:Message):Boolean = {
		subscribers.map(s => s.receive(name, message))
		true
	}
}

trait Message {
	type T
	val data:T
}

class StringMessage(val data:String) extends Message {
	type T = String
}

class StringSubscriber(id:String) extends Subscriber {
	def receive(topic:String, message:Message) {
		val d = message.data
		println("[%s] got message: %s".format(id, d))
	}
}

class StringPubSub extends PubSub {
	type T = String
}

abstract class PubSub {
	type T
	
	val ALL_OR_NONE_POLICY_ENABLED = true
	
	val topics = new ConcurrentRadixTree[Topic](new DefaultCharArrayNodeFactory())
	val subscribers = HashMap[Subscriber, String]()
	
	def publish(topic:String, message:Message):Boolean = {
		val t = topics.getValueForExactKey(topic)
		if (t != null) {
			t.publish(message)
		} else {
			false
		}
	}
	
	def subscribe(sub:Subscriber, pattern:String) : Boolean = {
		val res = findTopics(pattern).map(_.subscribe(sub)).forall(_ == true)
				
		if (!res && ALL_OR_NONE_POLICY_ENABLED) { 
			// could not subscribe to all topics,
			// consult all or none policy
			unsubscribe(sub, pattern)
			false
		}
		
		subscribers += (sub -> pattern)
		true
	}
	
	def unsubscribe(sub:Subscriber, pattern:String):Boolean = {
		true
	}
	
	def addTopic(topic:String):Boolean = {
		
		if (topics.getValueForExactKey(topic) == null) {
			val tp = new Topic(topic)
			topics.put(topic, tp)
			refreshSubscriptions(tp)
		}
		
		true
	}
	
	def refreshSubscriptions(topic:Topic):Boolean = {
		subscribers.map(kv => { subscribe(kv._1, kv._2) }).forall(_ == true)
	}
	
	def findTopics(pattern:String):Set[Topic] = {
		if (pattern.endsWith("*")) {
			val ts = topics.getValuesForKeysStartingWith(pattern.stripSuffix("*"))
			if (ts == null) Set[Topic]()
			else ts.toSet[Topic]
		} else {
			val t = topics.getValueForExactKey(pattern)
			if (t == null) Set[Topic]()
			else Set(t)
		}
	}
}

class KafkaApplier extends RawApplier {
    val logger = Logger.getLogger(getClass.getName)

    // Task management information.
    var taskId:Int = 0
    var serviceSchema:String = ""

    // Latest event.
    var latestHeader:ReplDBMSHeader = _            
        

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    def setTaskId(id:Int) {
        taskId = 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean, boolean)
     */
    def apply(event:DBMSEvent, header:ReplDBMSHeader, doCommit:Boolean, doRollback:Boolean) {
    	    	
    	if (latestHeader != null && 
    		latestHeader.getLastFrag() && 
    		latestHeader.getSeqno() >= header.getSeqno() && 
    		!(event.isInstanceOf[DBMSEmptyEvent])) {
            logger.info("Skipping over previously applied event: seqno=" + header.getSeqno() + " fragno=" + header.getFragno());
            return
        }
    	
        
    	val dbmsDataValues = event.getData()
    	    	
        // Iterate through values inferring the database name.
    	dbmsDataValues.foreach(dbmsData => {
    		
            if (dbmsData.isInstanceOf[StatementData]) {
            	
                logger.info("Ignoring statement %s".format(dbmsData.asInstanceOf[StatementData].getQuery()));
                
            } else if (dbmsData.isInstanceOf[RowChangeData]) {
            	
                val rd = dbmsData.asInstanceOf[RowChangeData]
                
                // we can get multiple row changes
                rd.getRowChanges().foreach(rowChange => {
                	try {
						val actionType = rowChange.getAction()
						val schema = rowChange.getSchemaName()
						val tableName = rowChange.getTableName()
						val colSpecs = rowChange.getColumnSpec()
																		
				        rowChange.getColumnValues().foreach(colVal => {
				        	
				        	val pkey = rowChange.getKeySpec().map(key => {			
				        		colVal.get(key.getIndex() - 1).getValue()
				        	}).mkString(":")
						                	
				        	val key = pkey
				        	val cf = tableName.toLowerCase().head.toString
				            val kv = new Array[String](colVal.size)
				
				        	var i = 0
				        	
				        	while(i < colVal.size) {
				        		val name = colSpecs.get(i).getName()
				        		val value = colVal.get(i).getValue() match {
				        			case null => null
				        			case v => v.toString
				        		}
				
				                kv(i) = "{'%s': '%s'}".format(name,value)
				                i += 1
				        	}
					        	
					        logger.info("{'action':'%s', 'database':'%s', 'table':'%s', 'cols':[%s]".format(actionType, rowChange, tableName, kv.mkString(", ")))					        
				        })			        
                	} catch {
						case e:Exception => logger.severe("%s: %s".format(e.getMessage(), e.getStackTraceString ))
					}
                })
            }
            else if (dbmsData.isInstanceOf[LoadDataFileFragment])
            {
                logger.info("Ignoring load data file fragment");
            }
            else if (dbmsData.isInstanceOf[RowIdData])
            {
                logger.info("Ignoring row ID data");
            }
            else
            {
                logger.warning("Unsupported DbmsData class: " + dbmsData.getClass().getName());
            }
        })

        // Mark the current header and commit position if requested.
        latestHeader = header
        
        if (doCommit) {
        	commit()
        }
    }
    
    def commit() {
        // If there's nothing to commit, go back.
        if (this.latestHeader == null ) return
        
        try
        {
            // Add applied latency so that we can easily track how far back each
            // partition is. If we don't have data we just put in zero.
            var appliedLatency = if (latestHeader.isInstanceOf[ReplDBMSEvent]) {
                (System.currentTimeMillis() - (latestHeader.asInstanceOf[ReplDBMSEvent]).getExtractedTstamp().getTime()) / 1000
            } else 0

            updateCommitSeqno(latestHeader, appliedLatency)
        }
        catch
        {
        	case e => throw new ApplierException("Unable to commit transaction: "+ e.getMessage(), e)
        }
    }
    
    def rollback() {
        // does nothing for now...
    	logger.info("rollback: not implemented")
    }

    def getLastEvent() : ReplDBMSHeader = {
    	 if (commitSeqnoTable == null) {
    		return null 
    	 }            

        try
        {
            return commitSeqnoTable.lastCommitSeqno(taskId)
        } catch {
        	case e => throw new ApplierException(e)
        }
    }

    override def configure(context:PluginContext) {
        runtime = context.asInstanceOf[ReplicatorRuntime]
    }

    var commitSeqnoTable:CommitSeqnoTable = _
    var db:String = _
    var conn:Database = _
    var statement:Statement = _
    var runtime:ReplicatorRuntime = _
    
    private def updateCommitSeqno(header:ReplDBMSHeader, appliedLatency:Long) {
        if (commitSeqnoTable != null) {
        	commitSeqnoTable.updateLastCommitSeqno(taskId, header, appliedLatency)
        }
    }    
    
    override def prepare(context:PluginContext) {
    	try
        {
    		db = context.getReplicatorSchemaName()
    		
            conn = DatabaseFactory.createDatabase(
            	runtime.getJdbcUrl(db),
            	runtime.getJdbcUser(),
            	runtime.getJdbcPassword())
            
            conn.connect(false)
            statement = conn.createStatement()
            
            commitSeqnoTable = new CommitSeqnoTable(conn, db, runtime.getTungstenTableType(), false)
    		
            commitSeqnoTable.prepare(taskId)
            latestHeader = commitSeqnoTable.lastCommitSeqno(taskId)            
        } catch {
        	case e => throw new ReplicatorException(
                    "Unable to connect to MySQL: url="
                    + runtime.getJdbcUrl(db) 
                    + ",user=" + runtime.getJdbcUser() 
                    + ",password" + runtime.getJdbcPassword(), e)        
        }
    }

    override def release(context:PluginContext) {
        if (commitSeqnoTable != null)
        {
            commitSeqnoTable.release()
            commitSeqnoTable = null
        }

        //currentOptions = null;

        statement = null
        if (conn != null)
        {
            conn.close()
            conn = null
        }
    }
}
