package com.mate1.tungsten

import java.util.ArrayList
import java.util.Iterator
import java.util.logging.Logger
import scala.collection.JavaConversions._
import com.continuent.tungsten.replicator.applier.ApplierException
import com.continuent.tungsten.replicator.event.ReplDBMSEvent
import com.continuent.tungsten.replicator.event.ReplDBMSHeader
import com.continuent.tungsten.replicator.event.DBMSEvent
import com.continuent.tungsten.replicator.dbms.StatementData
import com.continuent.tungsten.replicator.dbms.RowChangeData
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment
import com.continuent.tungsten.replicator.dbms.RowIdData

class SampleCheckpointingApplier extends CommitCheckpointingApplier {
    val logger = Logger.getLogger(getClass.getName)

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean, boolean)
     */
    def _apply(event:DBMSEvent, header:ReplDBMSHeader, doCommit:Boolean, doRollback:Boolean) {
    	
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
    }
}
