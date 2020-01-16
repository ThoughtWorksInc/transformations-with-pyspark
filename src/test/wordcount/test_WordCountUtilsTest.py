import unittest
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row

import thoughtworks.wordcount.utils as w

def dataframe_converter(df):
    col = df.columns
    collected_counts = df.collect()
    word_counts = []
    [word_counts.append((i.asDict().get(col[0]),i.asDict().get(col[1]))) for i in collected_counts]
    word_counts.sort
    return word_counts

class WordCountUtilsTest(unittest.TestCase):

    def setUp(self):
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter("ignore")
        self.spark = SparkSession.builder.appName("WordCountUtilsTest").getOrCreate()

    def tearDown(self):
        self.spark.stop()

# Split Words
    #@unittest.skip("Ignore test")
    def test_split_words_by_spaces(self):
        print("test_split_words_by_spaces")
        sample_data = [Row("test splitting a dataset of words by spaces")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        results_spark = w.splitWords(self.spark, sample_data_df)
        df_length = results_spark.count()
        self.assertEqual(df_length,8)

    #@unittest.skip("Ignore test")
    def test_split_words_by_period(self):
        print("test_split_words_by_period")
        sample_data = [Row("test.splitting.a.dataset.of.words.by.period")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        results_spark = w.splitWords(self.spark, sample_data_df)
        df_length = results_spark.count()
        self.assertEqual(df_length,8)

    #@unittest.skip("Ignore test")
    def test_split_words_by_comma(self):
        print("test_split_words_by_comma")
        sample_data = [Row("test,splitting,a,dataset,of,words,by,comma")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        results_spark = w.splitWords(self.spark, sample_data_df)
        df_length = results_spark.count()
        self.assertEqual(df_length,8)

    #@unittest.skip("Ignore test")
    def test_split_words_by_hyphen(self):
        print("test_split_words_by_hyphen")
        sample_data = [Row("test-splitting-a-dataset-of-words-by-hyphen")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        results_spark = w.splitWords(self.spark, sample_data_df)
        df_length = results_spark.count()
        self.assertEqual(df_length,8)

    #@unittest.skip("Ignore test")
    def test_split_words_by_semicolon(self):
        print("test_split_words_by_semicolon")
        sample_data = [Row("test;splitting;a;dataset;of;words;by;hyphen")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        results_spark = w.splitWords(self.spark, sample_data_df)
        df_length = results_spark.count()
        self.assertEqual(df_length,8)

# Count Words
    #@unittest.skip("Ignore test")
    def test_count_words_basic(self):
        print("test_count_words_basic")
        sample_data = [Row("a test of splitting a dataset of words by spaces")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        sample_data_df_split = w.splitWords(self.spark, sample_data_df)
        word_count_df = w.countByWord(self.spark, sample_data_df_split)
        actual_word_counts = dataframe_converter(word_count_df)
        counts = [word_count[1] for word_count in actual_word_counts]
        total_count = sum(counts)
        self.assertEqual(total_count,10)

    #@unittest.skip("Ignore test")
    def test_should_not_aggregate_dissimilar_words(self):
        print("test_should_not_aggregate_dissimilar_words")
        sample_data = [Row("a test of splitting a dataset of words by spaces")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        sample_data_df_split = w.splitWords(self.spark, sample_data_df)
        word_count_df = w.countByWord(self.spark, sample_data_df_split)
        actual_word_counts = dataframe_converter(word_count_df)
        expected_word_counts = [('a', 2), ('by', 1), ('dataset', 1), ('of', 2), ('spaces', 1), ('splitting', 1), ('test', 1), ('words', 1)]
        self.assertEqual(actual_word_counts,expected_word_counts)

    #@unittest.skip("Ignore test")
    def test_case_insensitivity(self):
        print("test_case_insensitivity")
        sample_data = [Row("A TEST OF SPLITTING A DATASET OF WORDS BY SPACES")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        sample_data_df_split = w.splitWords(self.spark, sample_data_df)
        word_count_df = w.countByWord(self.spark, sample_data_df_split)
        actual_word_counts = dataframe_converter(word_count_df)
        expected_word_counts = [('a', 2), ('by', 1), ('dataset', 1), ('of', 2), ('spaces', 1), ('splitting', 1), ('test', 1), ('words', 1)]
        self.assertEqual(actual_word_counts,expected_word_counts)

# Sort Words
    #@unittest.skip("Ignore test")
    def test_ordering_words(self):
        print("test_ordering_words")
        sample_data = [Row("a test of splitting a dataset of words by spaces")]
        sample_data_df = self.spark.createDataFrame(sample_data)
        sample_data_df_split = w.splitWords(self.spark, sample_data_df)
        word_count_df = w.countByWord(self.spark, sample_data_df_split)
        actual_word_counts = dataframe_converter(word_count_df)
        actual_word_order = [word_count[0] for word_count in actual_word_counts]
        expected_word_order = ['a', 'by', 'dataset', 'of', 'spaces', 'splitting', 'test', 'words']
        self.assertEqual(actual_word_order, expected_word_order)

if __name__ == '__main__':
    unittest.main()
