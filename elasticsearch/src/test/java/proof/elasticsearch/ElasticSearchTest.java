package proof.elasticsearch;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchTest {
    protected static Logger log = LoggerFactory.getLogger(ElasticSearchTest.class);
    private Node node;
    private static final String WORDS[] = {"У", "ПОПА", "БЫЛА", "СОБАКА", "организация"}; 

    @Test
    public void testCreateNode() {
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
        settings.put("name", "test");
        settings.put("path.home", "target/elastic");
        settings.put("node.data", "true");
        settings.put("cluster.name", "elasticsearch");
        node = NodeBuilder.nodeBuilder().loadConfigSettings(false).local(true).settings(settings).build();
        try {
            node.start();
            node.client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
            
            // удаляем все индексы
            node.client().admin().indices().prepareDelete().execute().actionGet();
            
            XContentBuilder source = XContentFactory.jsonBuilder()
           .startObject()
               .startObject("test_type")
                   .startObject("properties")
                       .startObject("f1")
                           .field("type", "string")
                           .field("index","analyzed")
                       .endObject()
                       .startObject("f2")
                            .field("type", "string")
                            .field("index","analyzed")
                            .field("analyzer","russian")
                       .endObject()
                   .endObject()
               .endObject()
            .endObject();    

            node.client().admin().indices().prepareCreate("test_nx").addMapping("test_type", source).execute().actionGet();
            
            BulkRequest bulkAddReq = new BulkRequest();
            for(int i = 0;i<10;i++) {
                IndexRequest indexReq = new IndexRequest()
                        .id(Integer.toString(i))
                        .index("test_nx")
                        .type("test_type")
                        .source("{\"f1\":\"test"+i+"v\",\"f2\":\""+WORDS[i % WORDS.length]+"\"}")
                        .create(true);

                bulkAddReq.add(indexReq );
            }
            BulkResponse bulkRes = node.client().bulk(bulkAddReq ).actionGet();
            for (BulkItemResponse item : bulkRes.items()) {
                if(item.failureMessage() != null) {
                    log.error(item.failureMessage());
                }
            }
            Assert.assertFalse(bulkRes.hasFailures());
            
            // дожидаемся окончания индексирования
            node.client().admin().indices().prepareRefresh().execute().actionGet();
            
            // проверяем количество записей в индексе
            checkCount();
            
            // проверяем работу стиминга английских слов
            checkEnglishAnalyzer();
            
            // проверяем работу стиминга русских слов
            checkRussianAnalyzer();
            
            // проверяем частичный поиск по английским словам
            checkSearchPartialEnglish();
            
            // проверяем частичный поиск по русским словам
            checkSearchPartialRussian();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        } finally {
            node.close();
        }
    }

    private void checkSearchPartialEnglish() {
        QueryBuilder queryBuilder = QueryBuilders.queryString("*est5*").field("f1").analyzeWildcard(true);
        SearchResponse searchResponse = node.client().prepareSearch("test_nx").setQuery(queryBuilder).execute().actionGet();
        
        Assert.assertEquals(1L,searchResponse.hits().totalHits());
        SearchHit hit0 = searchResponse.hits().getAt(0);
        Assert.assertEquals("5", hit0.getId());
    }

    private void checkSearchPartialRussian() {
        QueryBuilder queryBuilder = QueryBuilders.queryString("*обак*").field("f2");
        SearchResponse searchResponse = node.client().prepareSearch("test_nx").setQuery(queryBuilder).execute().actionGet();
        
        Assert.assertEquals(2L,searchResponse.hits().totalHits());
        Assert.assertEquals("3", searchResponse.hits().getAt(0).getId());
        Assert.assertEquals("8", searchResponse.hits().getAt(1).getId());
    }

    private void checkEnglishAnalyzer() {
        AnalyzeResponse analyseRes = node.client().admin().indices()
                .prepareAnalyze("test_nx", "READING testing testing123")
                .setField("f1")
                .setAnalyzer("english")
                .execute().actionGet();
        Assert.assertEquals("read", analyseRes.getTokens().get(0).getTerm());
        Assert.assertEquals("test", analyseRes.getTokens().get(1).getTerm());
        Assert.assertEquals("testing123", analyseRes.getTokens().get(2).getTerm());
    }

    private void checkRussianAnalyzer() {
        AnalyzeResponse analyseRes = node.client().admin().indices()
                .prepareAnalyze("test_nx", "СОБАКОЙ поп организацию")
                .setField("f2")
                .setAnalyzer("russian")
                .execute().actionGet();
        Assert.assertEquals("собак", analyseRes.getTokens().get(0).getTerm());
        Assert.assertEquals("поп", analyseRes.getTokens().get(1).getTerm());
        Assert.assertEquals("организац", analyseRes.getTokens().get(2).getTerm());
    }

    private void checkCount() {
        CountResponse countRes = node.client().prepareCount("test_nx").execute().actionGet();
        Assert.assertEquals(10, countRes.count());
    }

}
