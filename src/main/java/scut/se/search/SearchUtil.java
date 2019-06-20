package scut.se.search;

import com.hankcs.hanlp.HanLP;
import scut.se.dbutils.HBaseOperator;
import scut.se.dbutils.RowKeyGenerator;
import scut.se.dbutils.TableNameEnum;
import scut.se.entity.InvertedIndex;
import scut.se.entity.PageInfo;

import java.util.*;
import java.util.stream.Collectors;

public class SearchUtil {

    private static final int windowSize = 5;

    private static List<String> extractKeywordsFrom(String sentence) {
        return HanLP.extractKeyword(sentence, windowSize);
    }

    public static List<InvertedIndex> getInvertedIndices4Keywords(List<String> keywords) {
        HBaseOperator op = HBaseOperator.getInstance();
        return keywords.stream().map(kw -> {
            String rowKey = RowKeyGenerator.getHash(kw);
            InvertedIndex res = op.getColumnFamilyPOJOByRowKey(TableNameEnum.TABLE_SE, rowKey, InvertedIndex.class);
            return res;
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public static Map<PageInfo, Integer> getResult(String sentence) {
        List<PageInfo> pages = new ArrayList<>();
        List<String> keywords = extractKeywordsFrom(sentence);

        // 生成每个关键词的分数
        Map<String, Integer> kw2Score = new HashMap<>(keywords.size());
        for (int i = 0; i < keywords.size(); i++) kw2Score.put(keywords.get(i), keywords.size() - i);

        // 获取每个关键词的InvertedIndex
        List<InvertedIndex> invertedIndex4Kw = getInvertedIndices4Keywords(keywords);

        // 计算每个页面的分数
        Map<String, Integer> htmlNO2Score = new HashMap<>();
        invertedIndex4Kw.forEach(ii -> {
            String kw = ii.getWord();
            Integer kwScore = kw2Score.getOrDefault(kw, 0);

            List<String> htmlNOs = ii.getHtmlNOs();
            List<Integer> counts = ii.getCounts();

            for (int j = 0; j < htmlNOs.size(); j++) {
                String htmlNO = htmlNOs.get(j);
                Integer score = htmlNO2Score.getOrDefault(htmlNO, 0);
                if (score == null) score = 0;
                htmlNO2Score.put(htmlNO, score + kwScore * counts.get(j));
            }

        });

        return htmlNO2Score.entrySet().stream().flatMap(h2s -> {
            HBaseOperator op = HBaseOperator.getInstance();
            String htmlNO = h2s.getKey();
            Map<PageInfo, Integer> pi2s = new HashMap<>(1);
            PageInfo pageInfo = op.getColumnFamilyPOJOByRowKey(TableNameEnum.TABLE_HI, htmlNO, PageInfo.class);
            pi2s.put(pageInfo, h2s.getValue());
            return pi2s.entrySet().stream();
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
