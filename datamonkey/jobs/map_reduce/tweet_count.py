from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

# patterns = ["instance_id", "QosEvent"]
filler_words = ["umm", "us", "you", "i", "and", "what", "the", "fuck", "shit", "crap",
                "omg", "ditto", "of", "sort", "as", "are", "is", "should", "shall",
                "else", "then", "not", "ditto", "it", "stuff", "for", "if", "so", "wasn't",
                "isn't", "to", "that", "on", "at", "my", "in", "a", "fucking", "about", "your",
                "would", "could", "but", "yet", "your", "did", "it.", "sure", "with", "this", "just",
                "have", "was", "when", "from", "they", "how", "really", "will", "too", "even", "only",
                "had", "his", "her", "wanna", "off", "gonna", "them", "being", "someone", "some",
                "somebody", "its", "an"
                ]


class job(MRJob):

    def mapper_get_words(self, _, line):
        # yield each word in the line
        if len(line.split(",")) > 3:
            text = line.split(",")[2]
            for word in text.split(" "):
                if word.lower() not in filler_words:
                    word = word.strip()
                    if "@" not in word and "#" not in word and "/" not in word and "'" not in word and '"' not in word:
                        yield (word, 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)

    def steps(self):
        return [
            # self.mr(mapper=self.mapper_get_words,
            #         combiner=self.combiner_count_words,
            #         reducer=self.reducer_count_words),
            # self.mr(reducer=self.reducer_find_max_word)
            self.mr(mapper=self.mapper_get_words,
                    reducer=self.combiner_count_words)
        ]


if __name__ == '__main__':
    job.run()
