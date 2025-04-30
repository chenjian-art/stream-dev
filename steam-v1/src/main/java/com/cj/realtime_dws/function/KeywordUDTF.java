package com.cj.realtime_dws.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import com.cj.utils.KeywordUtil;

/**
 * @Package com.cj.realtime_dws.function.KeywordUDTF
 * @Author chen.jian
 * @Date 2025/4/14 11:31
 * @description: UDTF
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.get(text)) {
            collect(Row.of(keyword));
        }
    }
}

