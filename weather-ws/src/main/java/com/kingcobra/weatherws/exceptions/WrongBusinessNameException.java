package com.kingcobra.weatherws.exceptions;

/**
 * Created by kingcobra on 15/12/6.
 */
public class WrongBusinessNameException extends WeatherException {
    private String businessName;
    public WrongBusinessNameException(String businessName) {
        this.businessName = businessName;
    }

    @Override
    public String getMessage() {
        return "businessName is wrong or is not exist";
    }
}
