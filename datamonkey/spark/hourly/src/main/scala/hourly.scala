import java.util.Properties
import java.util.Calendar
import java.util.TimeZone

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.datastax.spark.connector._

// Terminal Command
// /Users/etseng/spark-1.2.0-bin-hadoop2.4/bin/spark-submit \
//   --master local[2] \
//   --class "HourlyMR" \
//   /Users/etseng/datamonkey/spark/hourly/target/scala-2.10/HourlyMR-assembly-1.0.jar /Users/etseng/datamonkey/spark/hourly/target/scala-2.10/hourlymr_2.10-1.0.jar

// /home/ubuntu/spark-1.2.0-bin-hadoop2.4/bin/spark-submit \
//   --master spark://ip-172-31-14-143:7077 \
//   --class "HourlyMR" \
//   /home/ubuntu/datamonkey/spark/hourly/target/scala-2.10/HourlyMR-assembly-1.0.jar /home/ubuntu/datamonkey/spark/hourly/target/scala-2.10/hourlymr_2.10-1.0.jar

object HourlyMR extends Serializable {

  private val AppName = "Hourly MR"

  // CREATE TABLE IF NOT EXISTS insight.conference_batch_details (
  //           yymmddhh text,
  //           location text,
  //           data map<text, int>,
  //           timestamp double,
  //           key text,
  //           PRIMARY KEY ((yymmddhh), location, key));


  // CREATE TABLE IF NOT EXISTS insight.conference_batch_details (
  //           yymmddhh text,
  //           timestamp double,
  //           lat float,
  //           lng float,
  //           data text,
  //           PRIMARY KEY ((yymmddhh), lat, lng, timestamp));

  def main(args: Array[String]) {

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    val conf = new SparkConf(true).setAppName(AppName)
                                  .set("spark.cores.max", "2")
                                  .set("spark.cassandra.connection.timeout_ms", "5000")
                                  .set("spark.cassandra.output.batch.size.bytes", "4096")
                                  .set("spark.cassandra.output.batch.size.rows", "1")
                                  .set("spark.cassandra.output.concurrent.writes", "2")
                                  .set("spark.cassandra.output.throughput_mb_per_sec option", "1")
                                  .set("spark.cassandra.connection.host", "54.67.74.224")
                                  .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    // Generate Date string for S3 path
    val mmddyy = generateS3Str()
    
    // Adapted from Word Count example on http://spark-project.org/examples/
    val file = sc.textFile("s3n://etseng-s3-oregon/test/*.csv")
    // val file = sc.textFile("s3n://etseng-s3-oregon/twitterstreaming/" + mmddyy + "/*.csv")

    // Extract coordinate and tweets from S3
    // val extractedData = file.map(line => mapdebug(line)).saveAsTextFile("/Users/etseng/result")
    val extractedData = file.map(line => mapData(line))

    // Group words by location
    val stage1ReducedByLocation = extractedData.reduceByKey(_ ::: _)

    // // Count words by location
    val stage2Reduced = stage1ReducedByLocation.map(data => reduceData(data))
    
    // // Save results to Cassandra
    // val cassFormat = stage2Reduced.map(data => formatToCassendra(data))
    
    // def saveToC(data: (String, String, Map[String, Int], Long, String)) = {
    //     val column = sc.parallelize(Seq(data))
    //     column.saveToCassandra("insight", "conference_batch_details", SomeColumns("yymmddhh", "location", "data", "timestamp", "key") )
    // }

    // cassFormat.map(data => saveToC(data))
    // stage2Reduced.map(data => formatToCassendra(data)).saveToCassandra("insight", "conference_batch_details", SomeColumns("yymmddhh", "location", "data", "timestamp", "key") )
    stage2Reduced.map(data => formatToCassendra(data)).saveToCassandra("insight", "test", SomeColumns("yymmddhh", "location", "data", "timestamp", "key") )
    // stage2Reduced.map(data => formatToCassendra(data)).saveAsTextFile("/Users/etseng/result")
  }



  def mapdebug(line: String): (String, List[String]) = {
    val filter_words = List("umm", "us", "you", "i", "and", "what", "the", "fuck", "shit", "crap",
                            "omg", "ditto", "of", "sort", "as", "are", "is", "should", "shall",
                            "else", "then", "not", "ditto", "it", "stuff", "for", "if", "so", "wasn't",
                            "isn't", "to", "that", "on", "at", "my", "in", "a", "fucking", "about", "your",
                            "would", "could", "but", "yet", "your", "did", "it.", "sure", "with", "this", "just",
                            "have", "was", "when", "from", "they", "how", "really", "will", "too", "even", "only",
                            "had", "his", "her", "wanna", "off", "gonna", "them", "being", "someone", "some",
                            "somebody", "its", "an", "absolutely", "ever", "why", "idk", "im", "be", "dont", "me")
    val lat_i = 0
    val lng_i = 1
    val text_i = 2
    
    val line_arr = line.split(",")

    if (line_arr.length < 10 || line_arr(lat_i) == "\u0007" || line_arr(lng_i) == "\u0007") {
      return ("0.00,0.00", List(""))
    }

    val listOfText = tokenize(line_arr(text_i)).filter(word => !filter_words.contains(word)) ::: List("OVERALL_CNT")
    // return (line_arr.length.toString, List(""))
    (line_arr(lng_i), listOfText) 
  }

  def mapData(line: String): (String, List[String]) = {

    val filter_words = List("umm", "us", "you", "i", "and", "what", "the", "fuck", "fucked", "shit", "crap",
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
    val lat_i = 0
    val lng_i = 1
    val text_i = 2
    
    val line_arr = line.split(",")

    if (line_arr.length < 3 || line_arr(lat_i) == "\u0007" || line_arr(lng_i) == "\u0007" || !isAllDigits(line_arr(lat_i)) || !isAllDigits(line_arr(lng_i))) {
      return ("0.00,0.00", List(""))
    }

    val listOfText = tokenize(line_arr(text_i)).filter(word => !filter_words.contains(word)).filter(word => !word.contains("http")) ::: List("OVERALL_CNT")
    val coordToStr = List("%.2f".format(line_arr(lat_i).toFloat), "%.2f".format(line_arr(lng_i).toFloat)).mkString(",")

    (coordToStr, listOfText)    
  }

  def generateYYMMDDHH(): String = {
    val timestamp: Long = System.currentTimeMillis
    val mydate = Calendar.getInstance()
    mydate.setTimeInMillis(timestamp)
    return List(mydate.get(Calendar.YEAR)%100, "%02d".format(mydate.get(Calendar.MONTH)+1), "%02d".format(mydate.get(Calendar.DAY_OF_MONTH)), "%02d".format(mydate.get(Calendar.HOUR_OF_DAY))).mkString("")
  }

  def generateS3Str(): String = {
    val timestamp: Long = System.currentTimeMillis
    val mydate = Calendar.getInstance()
    mydate.setTimeInMillis(timestamp)
    return List("%02d".format(mydate.get(Calendar.MONTH) + 1), "%02d".format(mydate.get(Calendar.DAY_OF_MONTH)), mydate.get(Calendar.YEAR)%100).mkString("")
  }

  def reduceData(json: (String, List[String])): (String, Map[String, Int]) = {
    val coord_key = json._1
    val reducedTuple = json._2.groupBy(word => word).map(mapword => (mapword._1, mapword._2.size))

    (coord_key, reducedTuple)
  }

  def formatToCassendra(data: (String,  Map[String, Int])): (String, String, Map[String, Int], Long, String) = {
  // def formatToCassendra(data: (String,  Map[String, Int])): (String, Float, Float, Long, String) = {

    // Get YYMMDDHH
    val timestamp: Long = System.currentTimeMillis
    val yymmddhh = generateYYMMDDHH()

    // Extract Lat and Lng from string
    // val geo_arr = data._1.split(",").map(x => x.toFloat)
    // val lat = geo_arr(0)
    // val lng = geo_arr(1)
    val location = data._1
    
    // Format list of words into string
    // val reducedStr = data._2.mkString(":").replace("(", "").replace(")", "")
    val map = data._2

    val key = data._2.toList.mkString(":").replace("(", "").replace(")", "")

    // (yymmddhh, lat, lng, timestamp, reducedStr)
    (yymmddhh, location, map, timestamp, key)
  }

  def isAllDigits(x: String): Boolean = {
    val arr = x.split('.')
    if (arr.length != 2) {
      return false
    }
    val tail = arr(1)
    return tail.matches("^\\d*$")
  }

  // Split a piece of text into individual words.
  private def tokenize(text : String) : List[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+").toList
  }  
} 

