from pyspark.sql import functions as F

WORD_COL = 'word'

def splitWords(spark, df):
    return df.select(F.split(df.columns[0], '[.]| |-|"|,|;').alias(WORD_COL)).select(F.explode(WORD_COL).alias(WORD_COL)).select(
        F.lower(F.col(WORD_COL)).alias(WORD_COL)).filter(F.col(WORD_COL) != "")

def countByWord(spark, df):
    return df.groupBy(WORD_COL).agg(F.count(WORD_COL)).alias('count').orderBy(WORD_COL)