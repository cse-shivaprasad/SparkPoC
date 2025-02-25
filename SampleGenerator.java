import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.*;

public class FinancialNetworkSettlement {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Financial Network Settlement PoC")
                .master("local[*]")
                .getOrCreate();

        List<Row> transactions = new ArrayList<>();
        StructType amountSchema = new StructType()
                .add("currency", DataTypes.StringType)
                .add("value", DataTypes.DoubleType);

        StructType additionalDetailsSchema = new StructType()
                .add("paymentMethod", DataTypes.StringType)
                .add("cardType", DataTypes.StringType)
                .add("merchantCategoryCode", DataTypes.StringType)
                .add("authCode", DataTypes.StringType);

        StructType customerInfoSchema = new StructType()
                .add("customerId", DataTypes.StringType)
                .add("customerSegment", DataTypes.StringType)
                .add("customerLocation", DataTypes.StringType);

        StructType riskDetailsSchema = new StructType()
                .add("fraudScore", DataTypes.IntegerType)
                .add("riskLevel", DataTypes.StringType)
                .add("chargebackRisk", DataTypes.StringType);

        StructType transactionSchema = new StructType()
                .add("transactionId", DataTypes.StringType)
                .add("merchantId", DataTypes.StringType)
                .add("acquirerId", DataTypes.StringType)
                .add("issuerId", DataTypes.StringType)
                .add("transactionDate", DataTypes.StringType)
                .add("transactionType", DataTypes.StringType)
                .add("amount", DataTypes.createArrayType(amountSchema))
                .add("status", DataTypes.StringType)
                .add("additionalDetails", additionalDetailsSchema)
                .add("customerInfo", customerInfoSchema)
                .add("riskDetails", riskDetailsSchema)
                .add("settlementDate", DataTypes.StringType)
                .add("processingFee", DataTypes.DoubleType);

        for (int i = 1; i <= 100; i++) {
            List<Row> amounts = Arrays.asList(
                    RowFactory.create("USD", Math.random() * 1000),
                    RowFactory.create("EUR", Math.random() * 1000)
            );

            Row additionalDetails = RowFactory.create("CreditCard", "VISA", "5411", "AUTH1234");
            Row customerInfo = RowFactory.create("CUST" + i, "Premium", "New York");
            Row riskDetails = RowFactory.create((int) (Math.random() * 100), "Medium", "Low");

            transactions.add(RowFactory.create(
                    UUID.randomUUID().toString(),
                    "M" + i,
                    "A" + i,
                    "I" + i,
                    "2025-02-25",
                    "PURCHASE",
                    amounts,
                    "SETTLED",
                    additionalDetails,
                    customerInfo,
                    riskDetails,
                    "2025-02-26",
                    Math.random() * 10
            ));
        }

        Dataset<Row> df = spark.createDataFrame(transactions, transactionSchema);
        df.write().mode(SaveMode.Overwrite).parquet("transactions.parquet");
        spark.stop();
    }
}
