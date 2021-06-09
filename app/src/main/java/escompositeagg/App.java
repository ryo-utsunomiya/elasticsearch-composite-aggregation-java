package escompositeagg;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class App {

  public static void main(String[] args) {
    var app = new App();
    app.loadData();
    System.out.println(app.getAllShopIdSet());
  }

  Set<Integer> getAllShopIdSet() {
    // aggregation クエリの組み立て
    final String aggName = "purchasedShops";
    final String fieldName = "shopId";
    final int aggregationSize = 10_000;
    CompositeAggregationBuilder aggregationBuilder = AggregationBuilders
        .composite(aggName, List.of(new TermsValuesSourceBuilder(fieldName).field(fieldName)))
        .size(aggregationSize);

    var result = new HashSet<Integer>();
    Map<String, Object> afterKey = null; // afterKey は初回リクエスト時にはnull
    CompositeAggregation aggregation;

    try (var client = new RestHighLevelClient(
        RestClient.builder(new HttpHost("localhost", 9200, "http")))) {

      // レスポンスに含まれる結果がaggregationのサイズを下回るまで、繰り返し取得する
      do {
        SearchResponse response = client.search(
            new SearchRequest()
                .indices("product")
                .source(
                    SearchSourceBuilder.searchSource().size(0)
                        .aggregation(aggregationBuilder.aggregateAfter(afterKey))
                ),
            RequestOptions.DEFAULT
        );
        aggregation = response.getAggregations().get(aggName);
        afterKey = aggregation.afterKey(); // afterKey は次のリクエストで使うので保存しておく
        result.addAll(
            aggregation.getBuckets()
                .stream()
                .map(bucket -> Integer.parseInt(bucket.getKey().get(fieldName).toString()))
                .collect(Collectors.toList())
        );
      } while (aggregation.getBuckets().size() >= aggregationSize);

    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  void loadData() {
    try (var client = new RestHighLevelClient(
        RestClient.builder(new HttpHost("localhost", 9200, "http")))) {

      var random = new Random();
      var objectMapper = new ObjectMapper();
      var bulkProcessor = createBulkProcessor(client);

      for (int i = 0; i < 100_000; i++) {
        var product = new Product();
        product.setProductId(random.nextInt(100_000));
        product.setShopId(random.nextInt(100_000));

        var request = new IndexRequest("product");
        request.source(objectMapper.writeValueAsBytes(product), XContentType.JSON);
        bulkProcessor.add(request);
      }
      bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);

    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static class Product {

    private int productId;
    private int shopId;

    public int getProductId() {
      return productId;
    }

    public void setProductId(int productId) {
      this.productId = productId;
    }

    public int getShopId() {
      return shopId;
    }

    public void setShopId(int shopId) {
      this.shopId = shopId;
    }
  }

  static BulkProcessor createBulkProcessor(RestHighLevelClient client) {
    return BulkProcessor
        .builder(
            ((bulkRequest, bulkResponseActionListener) ->
                client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener)),
            new BulkProcessListener()
        )
        .build();
  }

  static class BulkProcessListener implements BulkProcessor.Listener {

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
      System.out.println("Executing bulk [" + executionId + "] with ["
          + request.numberOfActions() + "] requests.");
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
      System.out.println(
          "Bulk [" + executionId + "] completed in " + response.getTook().getMillis() + "ms");
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

    }
  }
}
