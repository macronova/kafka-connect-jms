Connectivity with IBM MQ has been achieved by filesystem JNDI service. Binding file created with:

    (mq:9.1.0.0)root@ibmmq:/opt/mqm/java/bin# ./JMSAdmin
    InitCtx> DEFINE QCF(ConnectionFactory) QMGR(MQ1) tran(client) chan(DEV.APP.SVRCONN) host(ibmmq) port(1414)
    InitCtx> DEFINE Q(DEV.QUEUE.1) QUEUE(DEV.QUEUE.1) QMGR(MQ1)
    InitCtx> DEFINE Q(DEV.QUEUE.2) QUEUE(DEV.QUEUE.2) QMGR(MQ1)
    InitCtx> end

Docker manifest copies all configuration files to _/tmp/config_ directory (impacts sink and source JSON configuration).