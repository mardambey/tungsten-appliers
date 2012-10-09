package com.mate1.tungsten

import java.util.{UUID, Date}
import java.util.ArrayList
import java.util.Iterator
import java.util.logging.Logger

import scala.collection.JavaConversions._

import com.continuent.tungsten.replicator.applier.RawApplier
import com.continuent.tungsten.replicator.ReplicatorException
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

trait ApplierCallback {
	def apply(rowChange:OneRowChange)
}

trait InsertCallback extends ApplierCallback
trait UpdateCallback extends ApplierCallback
trait DeleteCallback extends ApplierCallback

class CallbackApplier extends RawApplier {
    val logger = Logger.getLogger(getClass.getName)

    // Task management information.
    var taskId:Int = 0
    var serviceSchema:String = ""

    // Latest event.
    var latestHeader:ReplDBMSHeader = _            
    var lastHeader:ReplDBMSHeader = _
    
    var callbacks:Option[List[ApplierCallback]] = _
    
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
                
                callbacks match {
                	case Some(xs) => {
                		rd.getRowChanges().foreach(rowChange => xs.foreach(callback => callback.apply(rowChange)))
                	}
                	
                	case _ =>                 		
                }
                
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

    def setCallbacks(strCallbacks:String) {
    	callbacks = Some(strCallbacks.split(",").map(callbackName => {
    		try {
    			Class.forName(callbackName).newInstance()    			
    		} catch {
    			case e:Exception => logger.severe("Could not instantiate callback %s: %s".format(callbackName, e.getMessage))
    			case e => logger.severe("Could not instantiate callback %s: %s".format(callbackName, e))
    		}
    	}).toList.asInstanceOf[List[ApplierCallback]])
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
