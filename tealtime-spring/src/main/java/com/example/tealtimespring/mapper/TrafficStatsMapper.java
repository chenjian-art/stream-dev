package com.example.tealtimespring.mapper;

import com.example.tealtimespring.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Package com.example.tealtimespring.mapper.TrafficStatsMapper
 * @Author chen.jian
 * @Date 2025/4/22 16:52
 * @description: 流量
 */

@Mapper
public interface TrafficStatsMapper {

    @Select("select ch,sum(uv_ct) uv_ct from dws_traffic_vc_ch_ar_is_new_page_view_window partition par#{date} " +
            "group by ch order by uv_ct desc limit #{limit}")
    List<TrafficUvCt> selectChUvCt(@Param("date") Integer date, @Param("limit") Integer limit);
}
