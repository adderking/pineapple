package com.kingcobra.weatherws.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kingcobra.weatherws.exceptions.WrongTimeRangeException;
import com.kingcobra.weatherws.utils.DateUtils;
import com.kingcobra.weatherws.utils.RegexUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Created by kingcobra on 15/12/8.
 */
@Component("businessRuleHelper")
public class BusinessRuleHelper {
    @Autowired
    private AreaHelper areaHelper;

    /**
     * 判断businessRule中的areaType,从相应的表中获取区域信息
     * @param businessRule
     * @param areaId
     * @return
     */
    public JSONObject getAreaInfo(JSONObject businessRule,String areaId,Constant.Language language) {
        String[] columns = null;
        Constant.AreaType areaType = getAreaType(businessRule);
        String nameColumn = getNameColumn(areaType, language);
        switch (language) {
            case CN:
                columns = new String[]{Constant.STATIONID,nameColumn,"districtcn","provcn"};break;
            case EN:
                columns = new String[] {Constant.STATIONID,nameColumn,"districten","proven"};break;
        }
        String tableName = getAreaTableName(areaType);
        JSONObject areaInfo = areaHelper.findAreaInfo(tableName, areaId,columns);
        return areaInfo;
    }

    /**
     * 获取业务规则中的时间范围
     * @param businessRule
     * @return 数组类型,所有需要查询的数据的时间范围，这个范围是右半开区间即[dates[0],dates[1])。
     */
    public String[] getTimeRange(JSONObject businessRule) {
        boolean isWeekend = businessRule.getBoolean(Constant.BUSINESSRULE_ISWEEKEND);
        String[] dates =null;
        if(isWeekend) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.MONTH, 6);
            calendar.set(Calendar.DAY_OF_MONTH, 31);
            calendar.set(Calendar.HOUR_OF_DAY, 10);
            calendar.getTime();
            dates = DateUtils.calTimeScope(calendar);
        }else {
            String startTime = businessRule.getString(Constant.BUSINESSRULE_STARTTIME);
            String endTime = businessRule.getString(Constant.BUSINESSRULE_ENDTIME);
            boolean isRight = RegexUtils.checkTimeInRule(startTime) & RegexUtils.checkTimeInRule(endTime);
            if (isRight) {
                dates = DateUtils.calTimeScope(startTime, endTime);
            }else {
                throw new WrongTimeRangeException(startTime,endTime);
            }
        }
         return dates;
    }

    /**
     * 根据业务规则获取周边区域的信息
     * @param businessRule
     * @return JSONArray,element为JSON格式,{stationId:,areaId:,areaName:}
     */
    public JSONArray getAroundArea(JSONObject businessRule,String areaId,Constant.Language language) {
        Constant.AreaType areaType = getAreaType(businessRule);
        boolean isAroundArea = businessRule.getBoolean(Constant.BUSINESSRULE_ISAROUNDAREA);
        boolean isAroundTravel = businessRule.getBoolean(Constant.BUSINESSRULE_ISAROUNDTRAVEL);
        String tableName = getAroundAreaTableName(areaType,isAroundArea,isAroundTravel);
        JSONArray result = areaHelper.findAroundArea(tableName, areaId);
        aroundAreaHandler(result, areaType,language);
        return result;
    }

    /**
     * 处理AroundArea表中的{areaId:stationId}格式数据,将其转换成{stationId:,areaId:,areaName:}格式
     * @param aroundAreaInfos
     * @param areaType
     * @return JSONArray
     */
    private void aroundAreaHandler(JSONArray aroundAreaInfos, Constant.AreaType areaType,Constant.Language language) {
        String tableName = getAreaTableName(areaType);
        String columnName = getNameColumn(areaType, language);
        String areaId, areaName;
        for (int i = 0; i < aroundAreaInfos.size(); i++) {
            areaId = aroundAreaInfos.getJSONObject(i).getString(Constant.AREAID_COLUMN);
            areaName = areaHelper.findAreaInfo(tableName, areaId,columnName).getString(columnName);
            aroundAreaInfos.getJSONObject(i).put(columnName, areaName);
        }

    }
    /**
     * 获取业务规则中定义的区域类型
     * @param businessRule
     * @return  AreaType对象
     */
    private Constant.AreaType getAreaType(JSONObject businessRule){
        String areaType = businessRule.getString(Constant.BUSINESSRULE_AREATYPE);
        Constant.AreaType _at = Constant.AreaType.valueOf(areaType);
        return _at;
    }

    /**
     * 根据areaType获得区域信息表名称
     * @param areaType
     * @retu
     */
    private String getAreaTableName(Constant.AreaType areaType) {
        String tableName = null;
        switch (areaType) {
            case internal:
                tableName = Constant.AREA_TABLE;break;
            case external:
                tableName = Constant.EXTERNAL_AREA_TABLE;break;
            case travel:
                tableName = Constant.INTERNAL_TRAVEL_TABLE;break;
            default:
                break;
        }
        return tableName;
    }

    /**
     * 根据areaType获得对应的周边区域表名称
     * @param areaType
     * @return
     */
    private String getAroundAreaTableName(Constant.AreaType areaType,boolean isAroundArea,boolean isAroundTravel) {
        String tableName = null;
        switch (areaType) {
            case internal:
                if(isAroundArea)
                    tableName = Constant.INTERNAL_AROUNDAREA_TABLE;
                else if(isAroundTravel)
                    tableName = Constant.INTERNAL_AROUNDTRAVEL_TABLE;
                break;
            case external:  //国外暂时没有周边景点
                tableName = Constant.EXTERNAL_AROUNDAREA_TABLE;
                break;
            case travel:
                if(isAroundArea)
                    tableName = Constant.INTERNAL_AROUNDAREA_TABLE;
                else if(isAroundTravel)
                    tableName = Constant.INTERNAL_AROUNDTRAVEL_TABLE;
                break;
            default:
                break;
        }
        return tableName;
    }

    /**
     * 根据areaType和language获得该区域信息name字段的字段名
     * @param areaType
     * @param language
     * @return
     */
    private String getNameColumn(Constant.AreaType areaType,Constant.Language language) {
        String nameColumn=null;
        switch (areaType) {
            case internal:
                switch (language) {
                    case CN:nameColumn = Constant.AREANAME_COLUMN_CN;break;
                    case EN:nameColumn = Constant.AREANAME_COLUMN_EN;break;
                    default:nameColumn = Constant.AREANAME_COLUMN_CN;break;
                }
                break;

            case external:
                switch (language) {
                    case CN:nameColumn = Constant.EXTERNAL_AREANAME_COLUMN_CN;break;
                    case EN:nameColumn = Constant.EXTERNAL_AREANAME_COLUMN_EN;break;
                    default:nameColumn = Constant.EXTERNAL_AREANAME_COLUMN_CN;break;
                }
                break;
            case travel:
                switch (language) {
                    case CN:nameColumn = Constant.INTERNAL_TRAVELNAME_COLUMN_CN;break;
                    case EN:nameColumn = Constant.INTERNAL_TRAVELNAME_COLUMN_EN;break;
                    default:nameColumn = Constant.INTERNAL_TRAVELNAME_COLUMN_CN;break;
                }
                break;
            default:nameColumn = Constant.AREANAME_COLUMN_CN;break;
        }
        return nameColumn;
    }
}