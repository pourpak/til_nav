import polars as pl
import nltk
import spacy
from gensim.models import Word2Vec
import logging

spacy.prefer_gpu()  # type: ignore

nlp = spacy.load("nb_core_news_lg")

lf: pl.LazyFrame = pl.scan_parquet("data/*.parquet").filter(
    pl.col("article_content").list.len() > 0
)

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)

# Pre-processing
trainer = nltk.data.load("data/punkt_norwegian.pk", format="pickle")
sent_detector = nltk.PunktSentenceTokenizer(trainer.get_params())  # type: ignore

text: pl.LazyFrame = lf.select(
    pl.col("article_content")
    .list.eval(pl.col("").list.get(1).str.concat(" ").str.to_lowercase())
    .list.first()
    .map_elements(
        lambda s: sent_detector.tokenize(s),
        return_dtype=pl.List(pl.Utf8),
        strategy="threading",
    )
).select(pl.col("article_content").explode().unique())

logging.info(".. Creating sentences and writing to disk")
# list_of_sentences: pl.DataFrame = text.collect(streaming=True)
# list_of_sentences.write_ndjson('data/sentences.ndjson')


logging.info(".. Lemmatizing sentences and writing to disk")
lemmatized_sentences: pl.LazyFrame = (
    pl.scan_ndjson("data/sentences.ndjson")
    .select(pl.col("article_content").unique())
    .select(
        pl.col("article_content").map_elements(
            lambda x: [token.lemma_ for token in nlp(x)],
            strategy="threading",
            return_dtype=pl.List(pl.Utf8),
        )
    )
)

# lemmatized_sentences.collect(streaming=True).write_ndjson('data/sentences_lemmatized.ndjson')


sentences_tokenized: list[str] = (
    pl.read_ndjson("data/sentences.ndjson").to_series().to_list()
)
vector_size: int = 500
model = Word2Vec(
    vector_size=vector_size, window=5, min_count=5, workers=6, seed=13, epochs=10
)

logging.info(".. Building vocabulary")
model.build_vocab(sentences_tokenized)

logging.info(".. Training model")
model.train(sentences_tokenized, total_examples=model.corpus_count, epochs=model.epochs)

model.save(f"models/word2vec_vector{vector_size}.model")
