package com.niuzhengyu.gmall0826.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    // 通过参数 日期 查询 phoenix 得到结果Double
    public Double selectOrderAmount(String date);

    // 通过参数 日期 查询 phoenix 得到结果 List代表很多行 map是每行里的数据 key 字段名 value 字段值
    public List<Map> selectOrderAmountHour(String date);
}
