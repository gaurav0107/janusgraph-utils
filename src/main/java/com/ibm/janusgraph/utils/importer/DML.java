package com.ibm.janusgraph.utils.importer;


/*
*
* Sample Vertex
*  {
      "custId": "1aslkdhj1i",
      "cust_ac_no": "1372382072",
      "fname": "Ankit",
      "lname": "Agrawal",
      "email": "ankit.agrwal24@gmail.com",
      "rtn": "8149971987",
      "dob": "13-12-1989",
      "gender": "U"
    },
*
* */


import com.ibm.janusgraph.utils.importer.Exception.CustomException;

import java.util.Iterator;
import java.util.Map;

public class DML {

    Iterator<Map<String, String>> records;

    public DML(){ }

    public DML(Iterator<Map<String, String>> r) {
        this.records = r;
    }


    public void post() throws Exception{
        if(this.records == null){
           throw new CustomException.DataNotFound("Invalid Data");
        }
    }

    public void patch() throws Exception{
        if(this.records == null){
            throw new CustomException.DataNotFound("Invalid Data");
        }
    }

    public void put() throws Exception{
        if(this.records == null){
            throw new CustomException.DataNotFound("Invalid Data");
        }
    }

    public void delete() throws Exception{
        if(this.records == null){
            throw new CustomException.DataNotFound("Invalid Data");
        }
    }


    public void get() throws Exception{

    }



}
