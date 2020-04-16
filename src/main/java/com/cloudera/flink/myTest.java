package com.cloudera.flink;

import java.nio.charset.StandardCharsets;

public class myTest {

    public static void main (String [] arge)
    {
        String message ="timestamp_ms\":1587030286697,\"created_at\":\"Thu Apr 16 09:44:46 UTC 2020\",\"amount\":735,\"text\":\"2970917765724948151\",\"id_customer\":4";
        //message.replaceAll("\"","");
        String[] split =message.split(",");
        String ts = split [0].substring(split[0].indexOf(":")+1);
        String cre = split[1].substring(split[1].indexOf(":")+1).replaceAll("\"","");
        String amm = split[2].substring(split[2].indexOf(":")+1);
        String tex = split[3].substring(split[1].indexOf(":")+1).replaceAll("\"","");
        String id = split[4].substring(split[4].indexOf(":")+1);

            for (String x:split)
            {
                System.out.println(x);
            }

    }
}
