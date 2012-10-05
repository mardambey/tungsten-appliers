package com.mate1.tungsten.callbacks

import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType
import com.continuent.tungsten.replicator.dbms.OneRowChange
import com.mate1.tungsten.DeleteCallback
import com.mate1.tungsten.InsertCallback
import com.mate1.tungsten.UpdateCallback
import java.util.logging.Logger

class LoggerCallback extends InsertCallback 
	with UpdateCallback 
	with DeleteCallback {
	
	val logger = Logger.getLogger(getClass.getName)
	
	def apply(rowChange:OneRowChange) {
		
		val actionType = rowChange.getAction()
		val schema = rowChange.getSchemaName()
		val table = rowChange.getTableName()
		
		// Fetch column names.
        val colSpecs = rowChange.getColumnSpec()

        // Make a document and insert for each row.
        val colValues = rowChange.getColumnValues().iterator()
                        
        while (colValues.hasNext()) {
        	val row = colValues.next()
        	
            val strBuf = new StringBuffer()
        	
        	for(i <- 0 until row.size) {
        		val name = colSpecs.get(i).getName()
        		val value = row.get(i).getValue()
        		        		
                strBuf.append("k=%s, v=%s".format(name,value))
        	}
        	
        	logger.info("action=%s database=%s table=%s cols={%s}".format(actionType, rowChange, table, strBuf))
        }
	}
}
