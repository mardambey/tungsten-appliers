package com.mate1.casserole

import java.util.{UUID, Date}

import scala.collection.JavaConversions._
import scala.util.parsing.json._

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.Future._

import org.codehaus.jackson.map.ObjectMapper

import com.netflix.astyanax.impl._
import com.netflix.astyanax.connectionpool._
import com.netflix.astyanax.connectionpool.impl._
import com.netflix.astyanax.connectionpool.exceptions._
import com.netflix.astyanax.thrift._
import com.netflix.astyanax._
import com.netflix.astyanax.query._
import com.netflix.astyanax.model._
import com.netflix.astyanax.serializers._
import com.netflix.astyanax.util._
import com.netflix.astyanax.retry._

/**
 * TODO: add call that returns X largest rows
 * TODO: create OA dropper
 */

case class Remove(cf:ColumnFamily[_, _], rowKey:Object, columnKey:Object)
case class Decrement(cf:ColumnFamily[_, _], rowKey:Object, columnKey:Object)


/**
 * Batches Cassandra operations by wrapping a mutation batch from Astyanax 
 * wrapping it in an Akka actor. Every time batchSize rows are mutated or 
 * when the actor is shut down it will execute the mutation.
 * 
 * TODO: add a timeout and flush if its reached.
 * 
 */
class CassandraBatcher(ks:Keyspace, batchSize:Int = 1000) extends Actor {
	
	println("Batcher started...")
	
	var mutation = ks.prepareMutationBatch()
	
	def receive = {
		case Remove(cf, rowKey, columnKey) => {
			//mutation.withRow(cf.asInstanceOf[ColumnFamily[Object, Object]], rowKey).deleteColumn(columnKey)
			maybeFlush match {
				case resp => sender ! resp
			}
		}
		
		case Decrement(cf, rowKey, columnKey) => {			
			//mutation.withRow(cf.asInstanceOf[ColumnFamily[Object, Object]], rowKey).incrementCounterColumn(columnKey, -1)
			maybeFlush match {
				case resp => sender ! resp
			}
		}
		
		case e => println("got unknown message: %s".format(e))
	}
	
	override def postStop = {
		flush
	}
	
	protected def flush : Int = {
		val oldSize = mutation.getRowCount()
		//mutation.execute()
		mutation = ks.prepareMutationBatch()
		oldSize
	}
	
	protected def maybeFlush : Int = {
		val ret = if (mutation.getRowCount() >= batchSize) {			
			try {
				flush
			} catch {
				case e:Exception => println(e.getMessage())
				0
			}				
		} else {			
			0
		}
		
		ret
	}
}

object CasseroleUtil {
	
	var port = 9160
	var maxConnsPerHost = 8
	var clusterName = "Test Cluster"
	var keyspaceName = "TestKeyspace"
	var socketTimeout = 600000
	var maxTimeoutCount = 9999999
	var timeoutWindow = 9999999
	var seeds = "127.0.0.1:9160"
		
	val numRowsPerFetch = 100
	val concurrencyLevel = 1
	var keyspace:Keyspace = _
	var batchSize = numRowsPerFetch * concurrencyLevel * 4
	var batcher:ActorRef = _ 
	
	val system = ActorSystem("Casserole")
	
	var context:AstyanaxContext[Keyspace] = _
	    
    /**
     * Connect to Cassandra.
     */
    def connect = {
		context = new AstyanaxContext.Builder()
		    .forCluster(clusterName)
		    .forKeyspace(keyspaceName)
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
		        .setDiscoveryType(NodeDiscoveryType.NONE)
		    )
		    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")    
		        .setPort(port)
		        .setMaxConnsPerHost(maxConnsPerHost)
		        .setSocketTimeout(socketTimeout)
		        .setMaxTimeoutCount(maxTimeoutCount)
		        .setTimeoutWindow(timeoutWindow)
		        .setSeeds(seeds)
		    )
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance())
		
		context.start
		
		keyspace = context.getEntity()
		batcher = system.actorOf(Props(new CassandraBatcher(keyspace, batchSize)))
	}
    
    /**
     * Shuts down the system and disconnects from Cassandra
     */
    def shutdown = {
		system.stop(batcher)		
		system.shutdown()
		context.shutdown()
		println("All cleaned up!")
	}
	
	def trimRows(cf:ColumnFamily[String, UUID], trimDate:Long, callback:(Rows[String, UUID] => Unit)) = {		
		
		println("Here we go... hold tight!")
		
		keyspace.prepareQuery(cf)			
			.withRetryPolicy(new ExponentialBackoff(250, 5))
			.getAllRows()
			.setRowLimit(numRowsPerFetch)
			.setRepeatLastToken(false)
			.setConcurrencyLevel(concurrencyLevel)
			.withColumnRange(new RangeBuilder().setLimit(50000).build())
			.executeWithCallback(new TrimRowCallback(cf, trimDate, callback))			
	}
}

class TrimRowCallback(cf:ColumnFamily[String, UUID], trimDate:Long, callback:(Rows[String, UUID] => Unit), retryOnFail:Boolean = true) 
	extends RowCallback[String, UUID]() {
		
	def success(rows:Rows[String, UUID]) {
		callback(rows)
	}	
	
	def failure(e:ConnectionException) : Boolean = {
		retryOnFail  // Returning true will continue, false will terminate the query
	}
}

class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }

object M extends CC[Map[String, Any]]
object L extends CC[List[Any]]
object S extends CC[String]
object D extends CC[Double]
object B extends CC[Boolean]

object Casserole extends App {
	import akka.util.duration._
	val trimDate = System.currentTimeMillis - (90 days).toMillis
	val feeds = new ColumnFamily[String, UUID]("feeds", StringSerializer.get(), TimeUUIDSerializer.get())
	val counters = new ColumnFamily[String, String]("counters", StringSerializer.get(), StringSerializer.get())
	
	CasseroleUtil.seeds = "cassandra1-new.mate1:9160,cassandra2-new.mate1:9160,cassandra3.mate1:9160,cassandra4.mate1:9160"
	CasseroleUtil.keyspaceName = "Mate1Feed"
	CasseroleUtil.maxConnsPerHost = 24
	
	CasseroleUtil.connect
	
	var totalFlushes = 0	
	var rowsExamined = 0
	
	implicit val timeout = Timeout(10 seconds)
	implicit val system = CasseroleUtil.system
	
	val statsPing = CasseroleUtil.system.scheduler.schedule(0 milliseconds,
		10 seconds,
		CasseroleUtil.system.actorOf(Props(new Actor() {
			def receive = {
				case "ping" => {
					println("Rows examined: %s".format(rowsExamined))
					println("Total flushes: %s".format(totalFlushes))
				}
			}
		})),
	"ping")
	
	val COUNTROLL = "count_roll"
	val COUNTREAD = "count_read"
	val COUNTALL = "count_all"							
	val ROLLEDUP = "ROLLEDUP"
	
	val mapper = new ObjectMapper()
	val factory = mapper.getJsonFactory()
	
	println("Cleaning up until %s".format(new Date(trimDate)))
	
	CasseroleUtil.trimRows(feeds, trimDate, (rows => {		
		try {
				
		rows.iterator.foreach(row => {
			rowsExamined += 1
			row.getColumns.foreach(c => {


				try {
					val time = TimeUUIDUtils.getTimeFromUUID(c.getName)
					
					if (time < trimDate)
					{
						val s_time = System.currentTimeMillis()
						
						val Array(userId, tier) = row.getKey.split(":")																		
						
						val flush = CasseroleUtil.batcher ? Remove(feeds, row.getKey(), c.getName)
						val future = tier match {
							
							case ROLLEDUP => {
								(CasseroleUtil.batcher ? Decrement(Casserole.counters, userId.toString, COUNTROLL)).mapTo[Int]
							}
							
							case tierN => {
								val json = c.getStringValue														
								val jp = factory.createJsonParser(json)
								val jsonObj = mapper.readTree(jp)
								val hasBeenRead = jsonObj.get("m_readFlag").asBoolean(false)								
																
								val readFuture = if (hasBeenRead) {
									(CasseroleUtil.batcher ? Decrement(Casserole.counters, userId.toString, COUNTREAD)).mapTo[Int]
								} else {
									Future { 0 }
								}

								
								val countAllFuture = tierN match {
									case "TIER1" => (CasseroleUtil.batcher ? Decrement(Casserole.counters, userId.toString, COUNTALL)).mapTo[Int]
									case _ => Future { 0 }
								}
								
								Future.sequence(List(countAllFuture, readFuture)).map(_.sum)																								
							}														
						}
						
						val count = Await.result(future, 5 seconds).asInstanceOf[Int]
						
						if (count > 0) {
							totalFlushes += count
							println("Status: current row key: %s".format(row.getKey()))
							println("Flushing done, count=%s, took %s millis, total flushes = %s.".format(count, (System.currentTimeMillis - s_time), totalFlushes))
						}						
					}
					
					//println("col: name=%s val=%s".format(new Date(c.getName.timestamp), c.getStringValue))
				} catch {
					case e:Exception => println(e.getMessage())
				}
			})
		})} catch { case e:Exception => println (e.getMessage)}
	}))
	
	sys.addShutdownHook({
		CasseroleUtil.shutdown
	}) 
}
