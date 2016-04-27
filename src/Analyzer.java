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
 * A worker process resides on an EC2 node. Its life cycle is as follows:
 * <p>
 * Repeatedly:
 * Get a message from an SQS queue.
 * Perform the requested job, and return the result.
 * remove the processed message from the SQS queue.
 */
public class Analyzer {
    private Analyzer() throws IOException {
        Utils.init();
    }

    private String getTweet(String link) throws IOException {
        // Get the page and parse it.
        Document doc = Jsoup.connect(link).get();
        // Extract page title.
        String tweet = doc.select("title").toString();
        // Clean string.
        return tweet.substring(7, tweet.length() - 8);
    }

    /**
     * Given a message of link, delete it from queue
     *
     * @param link link to delete
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

    private List printEntities(String tweet) {
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

    private void putAnswerInQueue(int sentiment, List entities, String key) {
        // Insert message to the queue.
        Utils.sqs_client.sendMessage(new SendMessageRequest(Utils.workers_manager_queue_url, key + "|" + sentiment + "|" + entities.toString()));
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
            System.out.println("body : " + key_link);

            // Message come without a key from getBody().
//            FIXME: ??? String key = key_link.split("\\|")[0];
            String link = key_link.split("\\|")[0];

            String tweet = analyzer.getTweet(link);

            int sentiment = analyzer.findSentiment(tweet);
            List entities = analyzer.printEntities(tweet);

            System.out.println("sentiment : " + sentiment);
            System.out.println("entities : " + entities);

            // Insert answer to answers queue.
            analyzer.putAnswerInQueue(sentiment, entities, message.getMessageId());
        }
    }
}
