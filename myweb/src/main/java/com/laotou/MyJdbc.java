package com.laotou;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class MyJdbc {
    public List<Area> getArea() throws Exception {
        List<Area> areaList=new ArrayList<>();
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.200.200/movie", "root", "Yfb010209");
        PreparedStatement ps = connection.prepareStatement("SELECT  * from movie_area");
        ResultSet rs = ps.executeQuery();
        Area area=null;
        while (rs.next()){
            area=new Area();
            area.setAreaName(rs.getString(2));
            area.setTicketPrice(rs.getString(3));
           areaList.add(area);
        }
        return areaList;
    }
}
