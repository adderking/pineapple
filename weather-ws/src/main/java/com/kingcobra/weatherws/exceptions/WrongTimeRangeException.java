package com.kingcobra.weatherws.exceptions;

/**
 * Created by kingcobra on 15/12/7.
 */
public class WrongTimeRangeException extends WeatherException {
    private String[] timeRange;

    public WrongTimeRangeException(String... timeRange) {
        this.timeRange = timeRange;
    }

    @Override
    public String getMessage() {
        return String.format("the format of timerange in business rule %s or %s is wrong.",timeRange[0],timeRange[1]);
    }

}
