tungsten-appliers
=================

Some appliers and other random code for Tungsten Replicator.

Sample configuration
====================

    replicator.pipeline.direct=binlog-to-q,q-to-callback
    replicator.pipeline.direct.stores=queue
    replicator.pipeline.direct.services=channel-assignment

    replicator.stage.binlog-to-q=com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask
    replicator.stage.binlog-to-q.extractor=dbms
    replicator.stage.binlog-to-q.applier=queue
    replicator.stage.binlog-to-q.filters=colnames,pkey

    replicator.stage.q-to-callback=com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask
    replicator.stage.q-to-callback.extractor=queue
    replicator.stage.q-to-callback.applier=callback

    # Callback applier
    replicator.applier.callback=com.mate1.tungsten.CallbackApplier
    replicator.applier.callback.callbacks=com.mate1.tungsten.callbacks.HBaseApplierCallback
    
    # HBase applier
    replicator.applier.hbase=com.mate1.tungsten.HBaseApplier
    replicator.applier.hbase.tables=table1,table2
    

