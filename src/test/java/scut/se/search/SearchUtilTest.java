package scut.se.search;

import com.hankcs.hanlp.HanLP;
import org.junit.*;
import scut.se.dbutils.HBaseOperator;
import scut.se.dbutils.HTableUntil;
import scut.se.dbutils.RowKeyGenerator;
import scut.se.dbutils.TableNameEnum;
import scut.se.entity.InvertedIndex;
import scut.se.entity.PageContent;
import scut.se.entity.PageInfo;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static scut.se.dbutils.TableNameEnum.TABLE_HI;
import static scut.se.dbutils.TableNameEnum.TABLE_SE;

public class SearchUtilTest {

    static private List<String> words = Arrays.asList("酷睿", "英特尔", "AMD", "性价比");
    static private List<String> rowKeyOfHI = null;
    static private List<String> rowKeyOfSE = null;
    static private List<InvertedIndex> invertedIndexList = null;
    static private List<Tuple<PageContent, PageInfo>> htmlInfoList = null;
    static final private int htmlNum = 10;
    private String sentence = "英特尔推出的新酷睿处理器性价比不如AMD的新处理器";

    @BeforeClass
    public static void initEntity() {
        HBaseOperator op = HBaseOperator.getInstance();
        // 查看是否已建表
        if (!HTableUntil.checkTableExist(TABLE_SE)) {
            op.createTable(TABLE_SE, Collections.singletonList(InvertedIndex.class.getName()));
        }
        if (!HTableUntil.checkTableExist(TABLE_HI)) {
            op.createTable(TABLE_HI, Arrays.asList(PageContent.class.getName(), PageInfo.class.getName()));
        }
        rowKeyOfSE = words.stream().map(RowKeyGenerator::getHash).collect(Collectors.toList());
        rowKeyOfHI = new ArrayList<>(htmlNum);
        htmlInfoList = new ArrayList<>(htmlNum);
        for (int i = 0; i < htmlNum; i++) {
            rowKeyOfHI.add(RowKeyGenerator.getUUID());
            String tmp = String.valueOf(i+1);
            PageInfo pageInfo = new PageInfo(tmp, tmp, tmp);
            PageContent pageContent = new PageContent(tmp, tmp);
            htmlInfoList.add(new Tuple<>(pageContent, pageInfo));
        }

        invertedIndexList = new ArrayList<>(words.size());
        for (int i = 0; i < words.size(); i++) {
            String word = words.get(i);
            Random random = new Random();
            StringBuilder htmlNOs = new StringBuilder();
            StringBuilder counts = new StringBuilder();
            int htmlNumBound = random.nextInt(10) + 1;
            for (int j = 0; j < htmlNumBound; j++) {
                int count = random.nextInt(10) + 1; // 大于1
                int htmlNOsListIndex = random.nextInt(10);
                htmlNOs.append(",");
                htmlNOs.append(rowKeyOfHI.get(htmlNOsListIndex));
                counts.append(",");
                counts.append(count);
            }
            InvertedIndex invertedIndex = new InvertedIndex(word, htmlNOs.substring(1), counts.substring(1));
            invertedIndexList.add(invertedIndex);
        }
    }

    @Before
    public void setUp() throws Exception {
        HBaseOperator op = HBaseOperator.getInstance();

        for (int i = 0; i < invertedIndexList.size(); i++) {
            op.insertOneRowTo(TABLE_SE, invertedIndexList.get(i), rowKeyOfSE.get(i));
        }

        for (int i = 0; i < htmlNum; i++) {
            Tuple<PageContent, PageInfo> tuple = htmlInfoList.get(i);
            PageInfo pageInfo = tuple._2();
            PageContent pageContent = tuple._1();
            op.insertOneRowTo(TABLE_HI, pageInfo, rowKeyOfHI.get(i));
            op.insertOneRowTo(TABLE_HI, pageContent, rowKeyOfHI.get(i));
        }
    }

    @After
    public void tearDown() throws Exception {
        HBaseOperator op = HBaseOperator.getInstance();

        // 删除测试用的实体类对应的记录
        for (String rowKey : rowKeyOfSE) {
            op.deleteRowByKey(TABLE_SE, rowKey);
        }

        for (String rowKey : rowKeyOfHI) {
            op.deleteRowByKey(TABLE_HI, rowKey);
        }
    }

    @Test
    public void getResult() {
        Map<PageInfo, Integer> res = SearchUtil.getResult(sentence);
        for (Map.Entry<PageInfo, Integer> entry: res.entrySet()) {
            PageInfo pageInfo = entry.getKey();
            Integer score = entry.getValue();
            System.out.println(MessageFormat.format("url: {0}, title: {1}, score: {2}", pageInfo.getUrl(), pageInfo.getTitle(), score));
        }
    }
}

class Tuple<X, Y> {
    private final X x;
    private final Y y;
    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public X _1() {
        return x;
    }

    public Y _2() {
        return y;
    }
}