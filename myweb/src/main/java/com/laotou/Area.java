package com.laotou;

public class Area {
    private String areaName;
    private String ticketPrice;

    public String getTicketPrice() {
        return ticketPrice;
    }

    public Area setTicketPrice(String ticketPrice) {
        this.ticketPrice = ticketPrice;
        return this;
    }

    public String getAreaName() {
        return areaName;
    }

    public Area setAreaName(String areaName) {
        this.areaName = areaName;
        return this;
    }

    @Override
    public String toString() {
        return "Area{" +
                "areaName='" + areaName + '\'' +
                ", ticketPrice='" + ticketPrice + '\'' +
                '}';
    }
}
