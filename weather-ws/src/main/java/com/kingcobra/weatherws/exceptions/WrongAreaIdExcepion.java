package com.kingcobra.weatherws.exceptions;

/**
 * Created by kingcobra on 15/12/6.
 */
public class WrongAreaIdExcepion extends WeatherException{

    private String areaId;
    public WrongAreaIdExcepion(String areaId) {
        this.areaId = areaId;
    }

    @Override
    public String getMessage() {
        return areaId + "is wrong or is not exist,please check it";
    }
}
