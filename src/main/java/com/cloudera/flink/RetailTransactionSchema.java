package com.cloudera.flink;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RetailTransactionSchema extends AbstractDeserializationSchema <RetailTransaction> {

   /*
    public long timestamp_ms;
    public String created_at;
    public int amount;
    public String text;
    public long  id_customer;
    */

   //timestamp_ms":1587030286697,"created_at":"Thu Apr 16 09:44:46 UTC 2020","amount":735,"text":"2970917765724948151","id_customer":4
    @Override
    public RetailTransaction deserialize(byte[] message) throws IOException {
       //System.out.println("#####################################################");

        String mes = new String (message,StandardCharsets.UTF_8);
        //mes.replaceAll("{","");
        //remove } from incoming JSON file
        mes.replaceAll("}","");
        String[] split = new String(message, StandardCharsets.UTF_8).split(",");;
        String ts = split [0].substring(split[0].indexOf(":")+1);
        String cre = split[1].substring(split[1].indexOf(":")+1).replaceAll("\"","");
        String amm = split[2].substring(split[2].indexOf(":")+1);
        String tex = split[3].substring(split[1].indexOf(":")+1).replaceAll("\"","");
        String id = split[4].substring(split[4].indexOf(":")+1,split[4].indexOf("}"));

        return new RetailTransaction(Long.parseLong(ts),cre, Integer.parseInt(amm), tex,Long.parseLong(id));
    }
}
