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

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes

class HBaseApplier extends RawApplier {
    val logger = Logger.getLogger(getClass.getName)

    // Task management information.
    var taskId:Int = 0
    var serviceSchema:String = ""

    // Latest event.
    var latestHeader:ReplDBMSHeader = _            
    var lastHeader:ReplDBMSHeader = _
    
    var tables:Option[Set[String]] = None
    
    val hbaseTables = HashMap[String, HTable]()
	
	val conf = HBaseConfiguration.create()
	val admin = new HBaseAdmin(conf)
    
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
    	
    	val dbmsDataValues = event.getData()
    	    	
        // Iterate through values inferring the database name.
    	dbmsDataValues.foreach(dbmsData => {
    		
            if (dbmsData.isInstanceOf[StatementData]) {
            	
                logger.fine("Ignoring statement");
                
            } else if (dbmsData.isInstanceOf[RowChangeData]) {
            	
                val rd = dbmsData.asInstanceOf[RowChangeData]
                
                rd.getRowChanges().foreach(rowChange => {
                	try {
						val actionType = rowChange.getAction()
						val schema = rowChange.getSchemaName()
						val tableName = rowChange.getTableName()
							
						if (tables.isEmpty || tables.isDefined && tables.get.contains(tableName)) {
							
							val table = hbaseTables.getOrElseUpdate(tableName.toLowerCase(), new HTable(conf, tableName.toLowerCase()))
							
					        val colSpecs = rowChange.getColumnSpec()
					                               
					        rowChange.getColumnValues().foreach(row => {
					        	
					        	val pkey = rowChange.getKeySpec().map(key => {			
					        		row.get(key.getIndex() - 1).getValue()
					        	}).mkString(":")
							                	
					        	val key = pkey
					        	val cf = tableName.toLowerCase().head.toString
					            val kv = new Array[String](row.size)
					        	val put = new Put(Bytes.toBytes(key))
					
					        	var i = 0
					        	
					        	while(i < row.size) {
					        		val name = colSpecs.get(i).getName()
					        		val value = row.get(i).getValue() match {
					        			case null => null
					        			case v => Bytes.toBytes(v.toString)
					        		}
					        		        		
					        		put.add(Bytes.toBytes(cf),Bytes.toBytes(name), value)
					
					                kv(i) = "{'%s': '%s'}".format(name,value)
					                i += 1
					        	}
					        	
					        	table.put(put)
					        	
					        	logger.fine("{'action':'%s', 'database':'%s', 'table':'%s', 'cols':[%s]".format(actionType, rowChange, tableName, kv.mkString(", ")))
					        })
						}
					} catch {
						case e:Exception => logger.severe("%s: %s".format(e.getMessage(), e.getStackTraceString ))
					}
                })                
            }
            else if (dbmsData.isInstanceOf[LoadDataFileFragment])
            {
                logger.fine("Ignoring load data file fragment");
            }
            else if (dbmsData.isInstanceOf[RowIdData])
            {
                logger.fine("Ignoring row ID data");
            }
            else
            {
                logger.warning("Unsupported DbmsData class: " + dbmsData.getClass().getName());
            }
        })

        // Mark the current header and commit position if requested.
        latestHeader = header
        
        if (doCommit) commit()
    }

    def setTables(strTables:String) {
    	tables = Some(strTables.split(",").toSet)
    }
    
    def commit() {
        // does nothing for now...
    }
    
    def rollback() {
        // does nothing for now...
    }

    def getLastEvent() : ReplDBMSHeader = lastHeader

    override def configure(context:PluginContext) {

    }

    override def prepare(context:PluginContext) {
        
    }

    override def release(context:PluginContext) {
        
    }
}
