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

abstract class CommitCheckpointingApplier extends RawApplier {
    private val logger = Logger.getLogger(getClass.getName)

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
     * Must be implemented by children.
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean, boolean)
     */
    protected def _apply(event:DBMSEvent, header:ReplDBMSHeader, doCommit:Boolean, doRollback:Boolean)
    
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
    	
        _apply(event, header, doCommit, doRollback)
            	
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
