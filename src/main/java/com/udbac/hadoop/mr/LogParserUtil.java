package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.QueryProperties;
import eu.bitwalker.useragentutils.UserAgent;
import jodd.util.StringUtil;
import jodd.util.URLDecoder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.stat.descriptive.StatisticalSummary;
import org.apache.log4j.Logger;
import com.udbac.hadoop.util.IPSeekerExt;
import com.udbac.hadoop.util.SplitValueBuilder;

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
        svb.add(handleQuery(st, QueryProperties.query()));
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

    private static SplitValueBuilder handleQuery(String analysedLog, String... keys) {
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
        for(String key:keys){
        analy.add(logMap.get(key));}
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
//    public static void main(String[] args){
//        String st = "2017-01-09 01:00:01 218.2.69.30 - - POST /page WT.a_cat=%E7%94%B5%E4%BF%A1&WT.a_nm=shapp&WT.a_pub=%E4%B8%AD%E5%9B%BD%E7%A7%BB%E5%8A%A8%E9%80%9A%E4%BF%A1%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8&WT.av=android4.1.1&WT.cg_n=%E5%BF%AB%E6%8D%B7%E5%85%A5%E5%8F%A3&WT.cid=866479026029271&WT.co=yes&WT.co_f=LIRZPjGmimk&WT.ct=WIFI&WT.dc=CMCC&WT.dl=60&WT.dm=x600&WT.ets=1483923599270&WT.ev=click&WT.event=%E5%A5%97%E9%A4%90%E4%BD%99%E9%87%8F&WT.g_co=cn&WT.mobile=14782376101&WT.nv=%E5%BF%AB%E6%8D%B7%E5%85%A5%E5%8F%A3_SF001_2_1&WT.os=6.0&WT.pi=%E9%A6%96%E9%A1%B5&WT.si_n=%E5%A5%97%E9%A4%90%E4%BD%99%E9%87%8F&WT.si_x=20&WT.sr=1080x1920&WT.sys=button&WT.ti=pagename&WT.tz=8&WT.uc=United+States&WT.ul=English&WT.vt_sid=LIRZPjGmimk.1483922551343&WT.vtid=LIRZPjGmimk&WT.vtvs=1483922551343 200 - HTTP/1.0 WebtrendsClientLibrary/v1.3.0.52+(App_Android) - - dcsgss24ish8yws4p4dtu8q7g_9q7p\n";
//        String[] k = QueryProperties.query();
//        System.out.println(handleQuery(st, k));
//    }
}
