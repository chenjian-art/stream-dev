package com.cj.realtime_dws.function;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.cj.realtime_dws.function.KeywordUtil
 * @Author chen.jian
 * @Date 2025/5/4 下午5:51
 * @description: 分词器
 */
public class KeywordUtil {
    public static List<String> analyze(String text){
        ArrayList<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(reader, false);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null) {
                String lexemeText = lexeme.getLexemeText();
                keywordList.add(lexemeText);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }

    public static void main(String[] args) {
        System.out.println(analyze("小米手机京东自营5G移动联通电信"));
    }
}
