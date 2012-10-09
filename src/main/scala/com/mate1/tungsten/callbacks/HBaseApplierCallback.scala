package com.mate1.tungsten.callbacks

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
import com.continuent.tungsten.replicator.dbms.OneRowChange
import com.mate1.tungsten.DeleteCallback
import com.mate1.tungsten.InsertCallback
import com.mate1.tungsten.UpdateCallback
import java.util.logging.Logger
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes
import com.continuent.tungsten.replicator.database.Table

class HBaseApplierCallback extends InsertCallback 
	with UpdateCallback 
	with DeleteCallback {
		
	val logger = Logger.getLogger(getClass.getName)
	
	val hbaseTables = HashMap[String, HTable]()
	
	val conf = HBaseConfiguration.create()
	val admin = new HBaseAdmin(conf)
	
	def apply(rowChange:OneRowChange) {
		
		try {
		val actionType = rowChange.getAction()
		val schema = rowChange.getSchemaName()
		val tableName = rowChange.getTableName()
																							
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
		} catch {
			case e:Exception => logger.severe("%s: %s".format(e.getMessage(), e.getStackTraceString ))
		}
	}
}
