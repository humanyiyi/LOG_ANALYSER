package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import eu.bitwalker.useragentutils.UserAgent;
import jodd.util.StringUtil;
import jodd.util.URLDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import util.IPSeekerExt;
import util.SplitValueBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/10.
 */
public class LogParserUtil {
    private final static Logger logger = Logger.getLogger(LogParserUtil.class);
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();

    public static SplitValueBuilder handleLog(String[] logTokens) {
        SplitValueBuilder svb = new SplitValueBuilder();
        String st;
        svb.add(logTokens[0]);
        svb.add(logTokens[1]);
        st = logTokens[2];
        svb.add(handleIP(st));
        svb.add(logTokens[2]);
        svb.add(logTokens[3]);
        svb.add(logTokens[4]);
        svb.add(logTokens[5]);
        svb.add(logTokens[6]);
        st = logTokens[7];
        svb.add(handleQuery(st));
        svb.add(logTokens[8]);
        st = logTokens[11];
        svb.add(handleUA(st));
        svb.add(logTokens[14]);
        return svb;

    }


    private static String handleUA( String usString) {
        String UA = null;
        if (StringUtil.isNotBlank(usString)) {
            UserAgent userAgent = UserAgent.parseUserAgentString(usString);
            UA = userAgent.getOperatingSystem().getName() + "|" + userAgent.getBrowser().getName();

        }
        return UA ;
    }

    private static SplitValueBuilder handleQuery(String analysedLog) {
        Map<String, String> logMap = new HashMap<>();
        String[] uriQuerys = StringUtils.split(String.valueOf(analysedLog), SDCLogConstants.QUERY_SEPARTIOR);
        for (String uriQuery : uriQuerys) {
            String[] uriItems = StringUtil.split(uriQuery, "=");
            if (uriItems.length == 2) {
                uriItems[1] = URLDecoder.decode(uriItems[1], "UTF-8");
                logMap.put(uriItems[0], uriItems[1]);
            }
        }
        Object s[] = logMap.keySet().toArray();
        SplitValueBuilder analy = new SplitValueBuilder();
        analy.add(logMap.get("WT.mobile"));
        analy.add(logMap.get("WT.nv"));
//        System.out.println(analy);
        return analy;
    }

    private static String handleIP(String uip) {
        String ip = null;
        if (StringUtil.isNotBlank(uip)) {
            IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(uip);
            if (info != null) {
                ip = info.getCountry() + "," + info.getProvince() + "," + info.getCity();
            }
        }
        return ip;
    }
}
