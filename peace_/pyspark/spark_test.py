from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover, NGram
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

# spark nlp basics
spark = SparkSession.builder.appName('nlp').getOrCreate()
sen_df = spark.createDataFrame([
    (0, "Hi, I head about Spark"),
    (1, "I wish java could use case classes"),
    (2, "Logistic,Regression,Classification,Models,Are,Clean")], ['id', 'sentence'])

# sen_df.show()

tokenizer = Tokenizer(inputCol='sentence', outputCol='words')
regex_tokenizer = RegexTokenizer(inputCol='sentence', outputCol='words', pattern='\\W')
count_tokens = udf(lambda words: len(words), IntegerType())

tokenized = tokenizer.transform(sen_df)
# tokenized.withColumn('tokens', count_tokens(col('words'))).show()

regex_tokenized = regex_tokenizer.transform(sen_df)
# regex_tokenized.withColumn('tokens', count_tokens(col('words'))).show()

# spark stopwords removal
sentence_df = spark.createDataFrame([
    (0, ['I', 'saw', 'the', 'green', 'horse']),
    (1, ['Mary', 'had', 'a', 'little', 'lamb'])
], ['id', 'tokens'])
remover = StopWordsRemover(inputCol='tokens', outputCol='filtered')
# remover.transform(sentence_df).show()

# n-gram
word_df = spark.createDataFrame([
    (0, ['Hi', 'i', 'head', 'about', 'Spark']),
    (1, ['I', 'wish', 'java', 'could', 'use', 'case', 'classes']),
    (2, ['Logistic', 'Regression', 'Classification', 'Models', 'Are', 'Clean'])], ['id', 'words'])
print(word_df)
ngram = NGram(n=2, inputCol='words', outputCol='grams')
print(ngram)
# ngram.transform(word_df).show(truncate=False)
