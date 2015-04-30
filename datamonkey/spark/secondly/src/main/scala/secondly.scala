import java.util.Properties
import java.util.Calendar
import java.util.TimeZone

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._


object SecondlyMR {

  // CREATE TABLE IF NOT EXISTS insight.conference_details (
  //           yymmddhh text,
  //           timestamp timestamp,
  //           lat float,
  //           lng float,
  //           data text,
  //           PRIMARY KEY ((yymmddhh), timestamp, lat, lng, data));
  
  def main(args: Array[String]) {

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));    

    val conf = new SparkConf().setAppName("SecondlyMR").set("spark.cassandra.connection.host", "54.67.74.224")
    val ssc = new StreamingContext(conf, Seconds(3))

    // val zk_url = "localhost:2181"
    val zk_url = "ec2-54-67-99-138.us-west-1.compute.amazonaws.com:2181"
    val groupID = "realtime"
    val topics_map = Map("twitterstreaming" -> 1)
    // val topics_map = Map("messages" -> 1)
    val kafkaStream = KafkaUtils.createStream(ssc, zk_url, groupID, topics_map)


    // Map reduce steps
    val parsedJson = kafkaStream.map(tweet => parse(tweet._2))
    
    val extractedData = parsedJson.map(json => (compact(render(json \ "geo" \ "coordinates")), compact(render( json \ "text" ))) ) 

    val stage1 = extractedData.map(eJson => mapData(eJson))
    
    val stage1ReducedByLocation = stage1.reduceByKey(_ ::: _)
    
    val stage2Reduced = stage1ReducedByLocation.map(data => reduceData(data))


    // Save to Cassendra
    stage2Reduced.map(data => formatToCassendra(data)).saveToCassandra("insight", "conference_details", SomeColumns("yymmddhh", "timestamp", "lat", "lng", "data") )
    
    ssc.start()
    ssc.awaitTermination()
  }

  def mapData(eJson: (String, String)): (String, List[String]) = {
    val filter_words = List("umm", "us", "you", "i", "and", "what", "the", "fuck", "shit", "crap",
                            "omg", "ditto", "of", "sort", "as", "are", "is", "should", "shall",
                            "else", "then", "not", "ditto", "it", "stuff", "for", "if", "so", "wasn't",
                            "isn't", "to", "that", "on", "at", "my", "in", "a", "fucking", "about", "your",
                            "would", "could", "but", "yet", "your", "did", "it.", "sure", "with", "this", "just",
                            "have", "was", "when", "from", "they", "how", "really", "will", "too", "even", "only",
                            "had", "his", "her", "wanna", "off", "gonna", "them", "being", "someone", "some",
                            "somebody", "its", "an", "absolutely", "ever", "why", "idk", "im", "be", "dont", "me",
                            "able", "about", "above", "abroad", "according", "accordingly", "across", "actually",
                            "adj", "after", "afterwards", "again", "against", "ago", "ahead", "aint", "all", "allow",
                            "allows", "almost", "alone", "along", "alongside", "already", "also", "although", "always", "am",
                            "amid", "amidst", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow",
                            "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear",
                            "appreciate", "appropriate", "are", "arent", "around", "as", "as", "aside", "ask", "asking", "associated",
                            "at", "available", "away", "awfully", "back", "backward", "backwards", "be", "became", "because", "become",
                            "becomes", "becoming", "been", "before", "beforehand", "begin",
                            "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief",
                            "but", "by", "came", "can", "cannot", "cant", "cant", "caption", "cause",
                            "causes", "certain", "certainly", "changes", "clearly", "cmon", "co", "co.", "com", "come", "comes", "concerning", "consequently",
                            "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "cs", "currently", "dare", "darent",
                            "definitely", "described", "despite", "did", "didnt", "different", "directly", "do", "does", "doesnt", "doing", "done",
                            "dont", "down", "downwards", "during",
                            "each", "edu", "eg", "eight", "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "entirely", "especially",
                            "et", "etc", "even", "ever", "evermore", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "fairly", "far",
                            "farther", "few", "fewer", "fifth", "first", "five", "followed", "following", "follows", "for", "forever", "former",
                            "formerly", "forth", "forward", "found", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes",
                            "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "half", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he",
                            "hed", "hell", "hello", "help", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "him", "himself",
                            "his", "hither", "hopefully", "how", "howbeit", "however", "hundred", "id", "ie", "if", "ignored", "ill", "im", "immediate", "in", "inasmuch",
                            "inc", "inc.", "indeed", "indicate", "indicated", "indicates", "inner", "inside", "insofar", "instead", "into", "inward", "is", "isnt", "it",
                            "itd", "itll", "its", "its", "itself", "ive", "just", "k", "keep", "keeps", "kept", "know", "known", "knows", "last", "lately", "later",
                            "latter", "latterly", "least", "less", "lest",
                            "let", "lets", "like", "liked", "likely", "likewise", "little", "look", "looking", "looks", "low", "lower", "ltd", "made", "mainly", "make",
                            "makes", "many", "may", "maybe", "maynt", "me", "mean", "meantime", "meanwhile", "merely", "might", "mightnt", "mine", "minus", "miss", "more",
                            "moreover", "most", "mostly", "mr", "mrs", "much", "must", "mustnt", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "neednt",
                            "needs", "neither", "never", "neverf", "neverless", "nevertheless", "new", "next", "nine", "ninety", "no",
                            "nobody", "non", "none", "nonetheless", "noone", "no-one", "nor", "normally", "not", "nothing", "notwithstanding", "novel", "now", "nowhere",
                            "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "ones", "only", "onto", "opposite", "or", "other",
                            "others", "otherwise", "ought", "oughtnt", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "past", "per",
                            "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provided", "provides", "que", "quite", "qv", "rather", "rd", "re",
                            "really", "reasonably", "recent", "recently", "regarding", "regardless", "regards", "relatively", "respectively", "right", "round", "said",
                            "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves",
                            "sensible", "sent", "serious", "seriously",
                            "seven", "several", "shall", "shant", "she", "shed", "shell", "shes", "should", "shouldnt", "since", "six", "so", "some", "somebody", "someday",
                            "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying",
                            "still", "sub", "such", "sup", "sure", "take", "taken", "taking", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thatll", "thats", "thats",
                            "thatve", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered", "therefore", "therein",
                            "therell", "therere", "theres", "theres", "thereupon", "thereve", "these", "they", "theyd", "theyll", "theyre", "theyve", "thing", "things",
                            "think", "third", "thirty", "this", "thorough", "thoroughly", "those", "though", "three", "through",
                            "throughout", "thru", "thus", "till", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts",
                            "twice", "two", "un", "under", "underneath", "undoing", "unfortunately", "unless", "unlike", "unlikely", "until", "unto", "up", "upon", "upwards",
                            "us", "use", "used", "useful", "uses", "using", "usually", "v", "value", "various", "versus", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way",
                            "we", "wed", "welcome", "well", "well", "went", "were", "were", "werent", "weve", "what", "whatever", "whatll", "whats", "whatve", "when",
                            "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "wheres", "whereupon", "wherever", "whether", "which", "whichever",
                            "while", "whilst", "whither", "who", "whod", "whoever", "whole", "wholl", "whom", "whomever", "whos", "whose", "why", "will", "willing",
                            "wish", "with", "within", "without", "wonder",
                            "wont", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "your", "youre", "yours", "yourself", "yourselves", "youve", "zero"
                            )
    
    val listOfText = tokenize(eJson._2).filter(word => !filter_words.contains(word)) ::: List("OVERALL_CNT")

    if (eJson._1.length != 0) {
      val coord_key = eJson._1.replace("[", "").replace("]", "").split(",").map(x => x.toDouble).map(x => "%.3f".format(x)).mkString(",")
      return (coord_key, listOfText)
    }

    return ("0.00,0.00", listOfText)
  }

  def reduceData(json: (String, List[String])): (String, List[(String, Int)]) = {
    val coord_key = json._1
    val reducedTuple = json._2.groupBy(word => word).map(mapword => (mapword._1, mapword._2.size)).toList

    (coord_key, reducedTuple)
  }

  def formatToCassendra(data: (String,  List[(String, Int)])): (String, java.sql.Timestamp, Float, Float, String) = {

    // Get YYMMDDHH
    val date = new java.util.Date()
    val datestamp = new java.sql.Timestamp(date.getTime)
    val timestamp: Long = System.currentTimeMillis
    val mydate = Calendar.getInstance()
    mydate.setTimeInMillis(timestamp)
    val yymmddhh = List(mydate.get(Calendar.YEAR)%100, "%02d".format(mydate.get(Calendar.MONTH)+1), "%02d".format(mydate.get(Calendar.DAY_OF_MONTH)), "%02d".format(mydate.get(Calendar.HOUR_OF_DAY))).mkString("")

    // Extract Lat and Lng from string
    val geo_arr = data._1.split(",").map(x => x.toFloat)
    val lat = geo_arr(0)
    val lng = geo_arr(1)
    
    // Format list of words into string
    val reducedStr = data._2.mkString(":").replace("(", "").replace(")", "")

    (yymmddhh, datestamp, lat, lng, reducedStr)
  }

  // Split a piece of text into individual words.
  private def tokenize(text : String) : List[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+").toList
  }  

}

// /Users/etseng/spark-1.2.0-bin-hadoop2.4/bin/spark-submit \
//   --class "SecondlyMR" \
//   --master local[2] \
//   --jars target/scala-2.10/SecondlyMR-assembly-1.0.jar target/scala-2.10/secondlymr_2.10-1.0.jar

