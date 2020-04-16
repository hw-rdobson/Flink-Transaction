package com.cloudera.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTransaction extends KeyedProcessFunction<Long,RetailTransaction,CustomerBalance> {


    private transient ValueState<CustomerBalance> balState;
    //private transient Histogram itemRead;
    //private transient Histogram itemWrite;

    @Override
    public void processElement(RetailTransaction transaction, Context ctx, Collector <CustomerBalance>out) throws Exception {
        long startTime = System.nanoTime();
        CustomerBalance balance =  balState.value();


        if (balance == null) {
            balance = new CustomerBalance(transaction.id_customer, 1000);
            //System.out.println("************************************************* CREATEING NEW BALANCE *************************************" );
        }

        balance.balance = balance.balance + transaction.amount;
        startTime = System.nanoTime();
        balState.update(balance);
        //itemWrite.update(System.nanoTime() - startTime);
        //System.out.println("************************************************* NEW BALANCE ************************************* " + balance.balance + "Customer id: "+ balance.customer_id );
        out.collect(balance);

    }
    @Override
    public void open(Configuration parameters) {
        // We create state read/write time metrics for later performance tuning

        balState = getRuntimeContext().getState(new ValueStateDescriptor<CustomerBalance>("customer_id", CustomerBalance.class));
    }



}
