package com.kingcobra.weatherws.common;

/**
 * Created by kingcobra on 15/12/6.
 */
public class Constant{
    //区域站点表
    public static final String AREA_TABLE = "area.internal:%s";

    //国内区域表中文名字段名称
    public static final String AREANAME_COLUMN_CN = "namecn";

    //国内区域表英文名字段名称
    public static final String AREANAME_COLUMN_EN = "nameen";
    //国内区域表区域ID字段名称
    public static final String AREAID_COLUMN = "areaId";

    //国外区域表
    public static final String EXTERNAL_AREA_TABLE = "area.external:%s";

    //国外区域表中文名字段名称
    public static final String EXTERNAL_AREANAME_COLUMN_CN = "namecn";

    //国外区域表中英名字段名称
    public static final String EXTERNAL_AREANAME_COLUMN_EN = "nameen";

    //国外区域表区域ID字段名称
    public static final String EXTERNAL_AREAID_COLUMN = "areaId";

    //国内景点表
    public static final String INTERNAL_TRAVEL_TABLE = "travel.internal:%s";

    //国内景点表中文名字段名称
    public static final String INTERNAL_TRAVELNAME_COLUMN_CN = "tnamecn";

    //国内景点表E文名字段名称
    public static final String INTERNAL_TRAVELNAME_COLUMN_EN = "tnameen";

    //国内景点表区域ID字段名称
    public static final String INTERNAL_TRAVELID_COLUMN = "tareaId";

    //周边站点表名
    public static final String INTERNAL_AROUNDAREA_TABLE = "area.internal.around:%s";

    //周边景点表名
    public static final String INTERNAL_AROUNDTRAVEL_TABLE = "travel.internal.around:%s";

    //国外周边区域表
    public static final String EXTERNAL_AROUNDAREA_TABLE = "area.external.around:%s";

    //气象站点字段名
    public static final String STATIONID = "stationId";

    //业务规则表
    public static final String BUSINESSRULE_TABLE = "businessRule:%s";

    /**
     * ---业务规则---
    */
    //数据库类型
    public static final String BUSINESSRULE_DBTYPE = "dbType";

    //查找的区域类型
    public static final String BUSINESSRULE_AREATYPE = "areaType";

    //业务规则字段，是否是周边站点
    public static final String BUSINESSRULE_ISAROUNDAREA = "isAroundArea";

    //是否是周末
    public static final String BUSINESSRULE_ISWEEKEND = "isWeekEnd";

    //是否查找周边景点
    public static final String BUSINESSRULE_ISAROUNDTRAVEL = "isAroundTravel";

    //业务数据表名
    public static final String BUSINESSRULE_DATATABLENAME = "tableName";

    //数据中表示预报时间的字段名称
    public static final String BUSINESSRULE_TIMECOLUMN = "timeColumn";

    //需要查询的数据列名
    public static final String BUSINESSRULE_COLUMNS = "columns";

    //数据开始时间
    public static final String BUSINESSRULE_STARTTIME = "startTime";

    //数据结束时间
    public static final String BUSINESSRULE_ENDTIME = "endTime";

    //BusinessRule中查询的区域类型：internal:国内，external:国外，travel：国内景点
    public enum AreaType{
        internal,external,travel
    }

    //Response相应状态
    public enum ResponseStatus {
        SUCCESS,FAILURE,ERROR;
        @Override
        public String toString() {
            return this.name().toLowerCase();
        }

    }
    //请求的语言类型：中文，英文
    public enum Language{
        CN,EN;

        @Override
        public String toString() {
            return this.name().toLowerCase();
        }
    }
    //数据时间的日期格式
    public static final String DATEFORMAT = "yyyyMMddHH";

    /* 预报数据中的属性名称*/
    //天气预报中白天黑天的标志，d:白天，n:黑天
    public static final char WEATHER_DAY = 'd';
    public static final char WEATHER_NIGHT = 'n';
    public static final String WEATHER_LEVEL = "weather"; //天气现象
    public static final String WEATHER_TIME = "weatherTime";
    public static final String WEATHER_CODE = "weatherCode";//天气代码: d01,n01 d表示白天，n表示夜晚,01表示天气现象
    public static final String WEATHER_DESC = "weatherDesc";//天气现象中文
    public static final String FF_LEVEL = "FF_LEVEL";   //风力代码
    public static final String FF_DESC = "ffDesc"; //风力中文
    public static final String DD_LEVEL = "DD_LEVEL";    //风向代码
    public static final String DD_DESC = "ddDesc"; //风向中文


    /*字典表名*/
    public static final String DICT_WEATHER = "dict:weather";//天气现象字典表
    public static final String DICT_FF = "dict:ff";//风速表
    public static final String DICT_DD = "dict:dd";//风向表

    /**
     * 缓存数据
     **/
    //两个参数：
    // %s为areaId.yyyyMMdd.d areaId为查询的区域ID，日期为当天的日期，d or n表示白天还是夜间
    // %s为语言类型：cn or en
    public static final String CACHE_FORECAST_KEYTEMPLATE = "cache:forecast:%s:%s";

    //监控表
    public static final String MONITORTABLENAME = "monitor:result:%s";
    public static final String MONITORETLTABLENAME = "monitor:etl:%s";
}
