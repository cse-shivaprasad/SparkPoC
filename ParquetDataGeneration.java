
import com.github.javafaker.Faker;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;

import java.util.*;

public class SettlementTransactionDataGenerator {

    public static void main(String[] args) {
        // Initialize Faker to generate random data
        Faker faker = new Faker();
        
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Settlement Transaction Data Generator")
                .config("spark.sql.warehouse.dir", "warehouseLocation")
                .getOrCreate();

        // Define number of records to generate
        int numRecords = 100; // Set your desired number of records

        // Prepare list to hold all generated data
        List<Row> dataRows = new ArrayList<>();

        // Create a pool of random merchant, acquirer, and issuer IDs
        List<String> merchantIds = new ArrayList<>();
        List<String> acquirerIds = new ArrayList<>();
        List<String> issuerIds = new ArrayList<>();

        // Populate the ID pools with some sample non-unique IDs
        for (int i = 0; i < 10; i++) {
            merchantIds.add(UUID.randomUUID().toString());
            acquirerIds.add(UUID.randomUUID().toString());
            issuerIds.add(UUID.randomUUID().toString());
        }

        // Generate the data
        for (int i = 0; i < numRecords; i++) {
            // Select random IDs for merchant, acquirer, issuer
            String merchantId = merchantIds.get(faker.number().numberBetween(0, merchantIds.size()));
            String acquirerId = acquirerIds.get(faker.number().numberBetween(0, acquirerIds.size()));
            String issuerId = issuerIds.get(faker.number().numberBetween(0, issuerIds.size()));

            String transactionId = UUID.randomUUID().toString();
            String transactionType = "purchase"; // or "refund"
            String currency = "USD";
            String transactionDate = faker.date().past(365, java.util.concurrent.TimeUnit.DAYS).toString();

            // Generate amounts (multiple values)
            List<Double> amounts = new ArrayList<>();
            amounts.add(faker.number().randomDouble(2, 50, 500));  // transaction amount
            amounts.add(faker.number().randomDouble(2, 1, 10));    // service fee
            amounts.add(faker.number().randomDouble(2, 5, 20));    // network fee

            // Generate nested fields (e.g., Merchant Details, Card Details, Fees, etc.)
            String merchantName = faker.company().name();
            String merchantCategoryCode = faker.number().digits(4); // Simulating a merchant category code
            String merchantLocation = faker.address().city();

            String cardNumber = faker.finance().creditCard();
            String cardType = faker.finance().creditCardType();
            String cardExpiration = faker.date().future(365, java.util.concurrent.TimeUnit.DAYS).toString();

            Double processingFee = faker.number().randomDouble(2, 2, 10); // Fee for processing
            Double serviceFee = faker.number().randomDouble(2, 1, 5);     // Service fee
            Double networkFee = faker.number().randomDouble(2, 3, 15);    // Network fee

            String issuerBank = faker.company().name();
            Double issuerFee = faker.number().randomDouble(2, 1, 10);

            String acquirerBank = faker.company().name();
            Double acquirerFee = faker.number().randomDouble(2, 1, 10);

            String networkType = "Visa"; // or "MasterCard"
            Double networkTransactionFee = faker.number().randomDouble(2, 2, 12);

            String approvalCode = faker.lorem().word();
            String statusFlag = "success"; // Can be success or failure

            // Prepare the row data to simulate the structure
            Row row = RowFactory.create(
                    transactionId, 
                    merchantId, 
                    acquirerId, 
                    issuerId, 
                    transactionType, 
                    currency, 
                    transactionDate, 
                    amounts,  // Amounts array
                    merchantName, 
                    merchantCategoryCode, 
                    merchantLocation, 
                    cardNumber, 
                    cardType, 
                    cardExpiration, 
                    processingFee, 
                    serviceFee, 
                    networkFee, 
                    issuerBank, 
                    issuerFee, 
                    acquirerBank, 
                    acquirerFee, 
                    networkType, 
                    networkTransactionFee, 
                    approvalCode, 
                    statusFlag
            );

            dataRows.add(row);
        }

        // Define the schema (as per the generated data structure)
        StructType schema = new StructType(new StructField[]{
                new StructField("transactionId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("merchantId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("acquirerId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("issuerId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("transactionType", DataTypes.StringType, false, Metadata.empty()),
                new StructField("currency", DataTypes.StringType, false, Metadata.empty()),
                new StructField("transactionDate", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Amounts", DataTypes.createArrayType(DataTypes.DoubleType), false, Metadata.empty()),

                new StructField("merchantName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("merchantCategoryCode", DataTypes.StringType, false, Metadata.empty()),
                new StructField("merchantLocation", DataTypes.StringType, false, Metadata.empty()),

                new StructField("cardNumber", DataTypes.StringType, false, Metadata.empty()),
                new StructField("cardType", DataTypes.StringType, false, Metadata.empty()),
                new StructField("cardExpiration", DataTypes.StringType, false, Metadata.empty()),

                new StructField("processingFee", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("serviceFee", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("networkFee", DataTypes.DoubleType, false, Metadata.empty()),

                new StructField("issuerBank", DataTypes.StringType, false, Metadata.empty()),
                new StructField("issuerFee", DataTypes.DoubleType, false, Metadata.empty()),

                new StructField("acquirerBank", DataTypes.StringType, false, Metadata.empty()),
                new StructField("acquirerFee", DataTypes.DoubleType, false, Metadata.empty()),

                new StructField("networkType", DataTypes.StringType, false, Metadata.empty()),
                new StructField("networkTransactionFee", DataTypes.DoubleType, false, Metadata.empty()),

                new StructField("approvalCode", DataTypes.StringType, false, Metadata.empty()),
                new StructField("statusFlag", DataTypes.StringType, false, Metadata.empty())
        });

        // Create DataFrame from the generated data rows
        Dataset<Row> df = spark.createDataFrame(dataRows, schema);

        // Save to Parquet format
        df.write().parquet("path/to/output/parquet");

        // Stop the Spark session
        spark.stop();
    }
}
