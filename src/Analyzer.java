import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Worker process.
 * Lifecycle:
 * - Get URL from SQS.
 * - Analyze tweet.
 * - Send result to answer SQS.
 * - Delete URL from SQS.
 */
public class Analyzer {

    private Analyzer() throws IOException {
        Utils.init();
    }

    /**
     * Get Tweet from a link.
     *
     * @param link URL to process.
     * @return Text of the tweet.
     * @throws IOException On HTTP failure.
     */
    private String getTweet(String link) throws IOException {
        // Get the page and parse it.
        Document doc = Jsoup.connect(link).get();
        // Extract page title.
        String tweet = doc.select("title").toString();
        // Clean string.
        return tweet.substring(7, tweet.length() - 8).replace('|', ' ').replace('\n', ' ').replace('\t', ' ');
    }

    /**
     * Given a message of link, delete it from queue
     *
     * @param link Link to delete
     */
    private void deleteLinkFromQueue(Message link) {
        Utils.sqs_client.deleteMessage(new DeleteMessageRequest(Utils.manager_workers_queue_url, link.getReceiptHandle()));
    }

    /**
     * Request a new link from the queue handler.
     *
     * @return Link to parse.
     */
    private Message getLinkFromSqs() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(Utils.manager_workers_queue_url);

        // Change the visibility of the message to 5 minutes.
        // A worker processes a message and then when finish, it deletes the message from the queue.
        // So in case a worker crashes, and the message wasn't deleted for 5 minutes,
        // then the other worker will see it again and process it
        receiveMessageRequest.setVisibilityTimeout(5 * 60);

        List<Message> messages = Utils.sqs_client.receiveMessage(receiveMessageRequest).getMessages();

        if (messages.size() == 0) {
            // Queue is empty.
            return null;
        }

        // Get the first message in the queue.
        return messages.get(0);
    }

    /**
     * Process the data from the tweet using CoreNLP.
     *
     * @param tweet String to process.
     * @return A sentiment score.
     */
    private int findSentiment(String tweet) {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP sentimentPipeline = new StanfordCoreNLP(props);

        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);

            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();

                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
            }
        }

        return mainSentiment;
    }

    /**
     * Get list of entities from a tweet.
     *
     * @param tweet tweet string.
     * @return List of entities.
     */
    private List getEntities(String tweet) {
        List<String> entities = new ArrayList<String>();

        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        StanfordCoreNLP NERPipeline = new StanfordCoreNLP(props);

        // create an empty Annotation just with the given text.
        Annotation document = new Annotation(tweet);

        // Run all Annotators on this text.
        NERPipeline.annotate(document);

        // These are all the sentences in this document.
        // A CoreMap is essentially a Map that uses class objects as keys and has values with custom types.
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // Traversing the words in the current sentence.
            // A CoreLabel is a CoreMap with additional token-specific methods.
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                // Text of the token.
                String word = token.get(TextAnnotation.class);
                // NER label of the token.
                String named_entity = token.get(NamedEntityTagAnnotation.class);
                if (named_entity.equals("PERSON") || named_entity.equals("LOCATION") || named_entity.equals("ORGANIZATION")) {
                    entities.add(word + ":" + named_entity);
                }
            }
        }

        return entities;
    }

    /**
     * Send result to the queue.
     *
     * @param sentiment
     *  Processed sentiment value.
     * @param entities
     *  Entities from the tweet.
     * @param key
     *  Message key.
     * @param tweet
     *  Tweet text.
     */
    private void putAnswerInQueue(int sentiment, List entities, String key, String tweet) {
        // Insert message to the queue.
        Utils.sqs_client.sendMessage(new SendMessageRequest(
                Utils.workers_manager_queue_url, key + "|" +
                sentiment + "|" +
                entities.toString() + "|" +
                tweet));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Init analyzer.
        Analyzer analyzer = new Analyzer();
        while (true) {
            // Get link to analyze from SQS.
            Message message = analyzer.getLinkFromSqs();

            // if queue is empty for next message..
            if (message == null) {
                Thread.sleep(500);
                continue;
            }

            // Delete link from queue.
            analyzer.deleteLinkFromQueue(message);

            // Extract tweet from link.
            String key_link = message.getBody();
            System.out.println("Body : " + key_link);

            // Message come without a key from getBody().
            String[] body_content =  key_link.split("\\|");
            String message_id = body_content[0];
            String link = body_content[1];

            String tweet = analyzer.getTweet(link);

            int sentiment = analyzer.findSentiment(tweet);
            List entities = analyzer.getEntities(tweet);

            System.out.println("Sentiment : " + sentiment);
            System.out.println("Entities : " + entities);
            System.out.println("Tweet : " + tweet);

            // Insert answer to answers queue.
            analyzer.putAnswerInQueue(sentiment, entities, message_id, tweet);
            System.out.println("Answered");

            // Remove processed message from SQS
            analyzer.deleteMessageFromWorkersQueue(message);
            System.out.println("Deleted the message");
        }
    }

    private void deleteMessageFromWorkersQueue(Message message) {
        DeleteMessageRequest dms = new DeleteMessageRequest(Utils.manager_workers_queue_url, message.getReceiptHandle());
        Utils.sqs_client.deleteMessage(dms);
    }
}
