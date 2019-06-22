package scut.se.search;

import com.hankcs.hanlp.HanLP;
import scut.se.dbutils.HBaseOperator;
import scut.se.dbutils.RowKeyGenerator;
import scut.se.dbutils.TableNameEnum;
import scut.se.dbutils.Tuple;
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

    public static List<Tuple<PageInfo, Integer>> getResult(String sentence) {
        List<PageInfo> pages = new ArrayList<>();
        List<String> keywords = extractKeywordsFrom(sentence);
        HBaseOperator op = HBaseOperator.getInstance();

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

        return htmlNO2Score.entrySet().stream().map(h2s -> {
            String htmlNO = h2s.getKey();
            Tuple<PageInfo, Integer> pi2s = null;
            PageInfo pageInfo = op.getColumnFamilyPOJOByRowKey(TableNameEnum.TABLE_HI, htmlNO, PageInfo.class);
            pi2s = new Tuple<>(pageInfo, h2s.getValue());
            return pi2s;
        }).sorted(Comparator.comparing(Tuple::_2, (last, cur) -> cur -last)).collect(Collectors.toList()); // 增加排序
    }

    public static List<Tuple<PageInfo, Integer>> getFeakerResult(String sentence) {
        List<Tuple<PageInfo, Integer>> resList = new ArrayList<>();
        resList.add(new Tuple<PageInfo, Integer>(new PageInfo("http://feaker.com", "feakerTitle", "feakerFilename"), 3));
        resList.add(new Tuple<PageInfo, Integer>(new PageInfo("http://feaker.com", "feakerTitle", "feakerFilename"), 3));
        resList.add(new Tuple<PageInfo, Integer>(new PageInfo("http://feaker.com", "feakerTitle", "feakerFilename"), 3));
        resList.add(new Tuple<PageInfo, Integer>(new PageInfo("http://feaker.com", "feakerTitle", "feakerFilename"), 3));
        resList.add(new Tuple<PageInfo, Integer>(new PageInfo("http://feaker.com", "feakerTitle", "feakerFilename"), 3));

        return resList;
    }
}
