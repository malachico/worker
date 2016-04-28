import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;

import java.io.IOException;

/**
 * Common Amazon objects for local app, manager and workers for saving code.
 */
class Utils {

    static AmazonEC2 ec2_client;
    static AmazonSQS sqs_client;
    static AmazonS3 s3_client;
    static AWSCredentials credentials;

    // queue urls: x_y_queue_url means x-->y direction queue
    static String local_manager_queue_url;
    static String manager_local_queue_url;
    static String workers_manager_queue_url;
    static String manager_workers_queue_url;

    public static String worker_user_data;
    public static String manager_user_data;

    static void init() throws IOException {
        System.out.println("Init credentials");
        initCredentials();

        System.out.println("Init S3");
        initS3();

        System.out.println("Init EC2 Client");
        initEC2Client();

        System.out.println("Init SQS");
        initSqs();

        System.out.println("Initialization done.");
    }

    /**
     * Initiate credentials from file.
     */
    private static void initCredentials() throws IOException {
        credentials = new PropertiesCredentials(
                Utils.class.getResourceAsStream("AwsCredentials.properties"));
    }

    private static void initS3() {
        s3_client = new AmazonS3Client(credentials);
    }

    private static void initSqs() throws IOException {
        // Create a queue
        sqs_client = new AmazonSQSClient(credentials);

        local_manager_queue_url = getQueue("local_manager_queue");
        manager_local_queue_url = getQueue("manager_local_queue");
        manager_workers_queue_url= getQueue("manager_workers_queue");
        workers_manager_queue_url= getQueue("workers_manager_queue");
//        Utils.clearAllSQS();
    }


    private static void initEC2Client() throws IOException {
        // Set client connection
        ec2_client = new AmazonEC2Client(credentials);
        ec2_client.setEndpoint("ec2.us-west-2.amazonaws.com");
    }

    /**
     * Gets a queue by name. Creates one if does not exist.
     *
     * @param name Name of the queue.
     * @return Queue URL.
     */
    private static String getQueue(String name) {
        try {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(name);
            return sqs_client.createQueue(createQueueRequest).getQueueUrl();
        }
        catch (Exception e) {
            return sqs_client.getQueueUrl(name).getQueueUrl();
        }
    }

    /**
     * Tag a remote instance.
     *
     * @param instanceId the instance to tag
     * @param tag        the tag to give to the instance ( example: "name" )
     * @param value      the value of the tag ( example: "worker" )
     */
    public static void tagInstance(String instanceId, String tag, String value) {
        CreateTagsRequest request = new CreateTagsRequest();
        request = request.withResources(instanceId)
                .withTags(new Tag(tag, value));
        Utils.ec2_client.createTags(request);
    }

    /**
     * Clear queue for debugging.
     */
    public static void clearSQS(String queueUrl){
        sqs_client.purgeQueue(new PurgeQueueRequest(queueUrl));

    }

    /**
     * Debug only; Clear all queues.
     */
    public static void clearAllSQS() {
        Utils.clearSQS(Utils.manager_local_queue_url);
        Utils.clearSQS(Utils.local_manager_queue_url);
        Utils.clearSQS(Utils.manager_workers_queue_url);
        Utils.clearSQS(Utils.workers_manager_queue_url);
        System.out.println("ALL SQS CLEARED");
    }
}
