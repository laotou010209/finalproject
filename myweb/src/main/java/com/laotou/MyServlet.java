package com.laotou;

import com.alibaba.fastjson.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

@WebServlet(name = "MyServlet",value = "/MyServlet")
public class MyServlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        MyJdbc jdbc=new MyJdbc();
        List<Area> areaList = null;
        try {
            areaList = jdbc.getArea();
        } catch (Exception e) {
            e.printStackTrace();
        }
        JsonResult jsonResult=new JsonResult();
        Area area=null;
        String[] data1=new String[areaList.size()];
        String[] data2=new String[areaList.size()];
        for(int i=0;i<areaList.size();i++){
             area = areaList.get(i);
             data1[i]=area.getAreaName();
             data2[i]=area.getTicketPrice();
        }
        jsonResult.setData1(data1);
        jsonResult.setData2(data2);
        PrintWriter writer = response.getWriter();
        //将对象转为json
        String jr = JSONObject.toJSONString(jsonResult);
        writer.print(jr);
        writer.flush();
        writer.close();
    }
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request,response);
    }
}
