import polars as pl
import lxml.etree as etree
from nltk.tokenize import sent_tokenize


def get_article_content(html_string: str) -> list[list[str]]:
    html = etree.HTML(html_string)
    try:
        return [[i.tag, i.text, str(i.attrib)] for i in html.xpath("//article//*")]  # type: ignore
    except AttributeError:
        return [["None"]]


def process_article_content(series: pl.Series) -> pl.Series:
    return series.list.eval(
        pl.col("")
        .filter(~filter_mask.cast(pl.Boolean))
        .filter(pl.col("").list.get(1).str.strip_chars().str.len_bytes() > 0)
        .filter(
            ~pl.col("").list.get(1).str.contains("Les hele saken med abonnement").any()
        )
    ).list.eval(pl.col("").list.eval(pl.col("").str.strip_chars()))


filter_mask = (
    pl.col("").list.get(2).str.contains(".*'class': 'title'.*")
    + pl.col("").list.get(2).str.contains(r"'class': 'plug-\w+")
    + pl.col("").list.get(2).str.contains(".*'class': 'article-publish-date.*'.*")
    + pl.col("").list.get(2).str.contains(r".'class': 'compilation-\w+'")
    + pl.col("").list.get(2).str.contains(".'class': 'autonomous.*'")
    + pl.col("").list.get(2).str.contains(".'class': 'nrk-button.*'")
    + pl.col("").list.get(2).str.contains(r"\{'class': '.*widget.*'}")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'nrk-sr'}")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'button-icon'}")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'article-.*'}")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'video-.*'}")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'bulletin-publish-.*'}")
    + pl.col("").list.get(2).str.contains(r".*'class': 'authors?__.*'}")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'fact__.*'}")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'sr-only'}")
    + pl.col("").list.get(2).str.contains(r"\'class': '.*skin.*'")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'link__container.*'}")
    + pl.col("").list.get(2).str.contains(r"\.*'itemprop': 'name'.*")
    + pl.col("").list.get(2).str.contains(r"\.*'itemprop': 'email'.*")
    + pl.col("").list.get(2).str.contains(r"\{'class': 'lp_.*'}")
    + pl.col("").list.get(2).str.contains(r"\.*'class': 'byline-name.*'}")
    + pl.col("").list.get(2).str.contains(r"\.*'class': '.*teaser.*'}")
    + pl.col("").list.get(2).str.contains(r"\.*'class': '.*_title.*'")
    + pl.col("").list.get(2).str.contains(r"\.*'class': '.*_button.*'")
    + pl.col("").list.get(2).str.contains(r"\.*'class': 'image-.*'")
    + pl.col("").list.get(2).str.contains(r"\.*'class': 'related-article'")
    + pl.col("").list.get(2).str.contains(r"\.*'data-test-tag': 'internal-link'")
    + pl.col("").list.get(0).str.contains("^div$")
    + pl.col("").list.get(0).str.contains("^header$")
    + pl.col("").list.get(0).str.contains("^h1$")
    + pl.col("").list.get(0).str.contains("^time$")
    + pl.col("").list.get(0).str.contains("^aside$")
    + pl.col("").list.get(0).str.contains("^button$")
    + pl.col("").list.get(0).str.contains("^svg$")
    + pl.col("").list.get(0).str.contains("^title$")
    + pl.col("").list.get(0).str.contains("^path$")
    + pl.col("").list.get(0).str.contains("^footer$")
    + pl.col("").list.get(0).str.contains("^use$")
    + pl.col("").list.get(0).str.contains("^img$")
    + pl.col("").list.get(0).str.contains("^link$")
    + pl.col("").list.get(0).str.contains("^script$")
    + pl.col("").list.get(0).str.contains("^figure$")
    + pl.col("").list.get(0).str.contains("^noscript$")
    + pl.col("").list.get(0).str.contains("^ul$")
    + pl.col("").list.get(0).str.contains("^li$")
    + pl.col("").list.get(0).str.contains("^amedia.*$")
    + pl.col("").list.get(0).str.contains("^style$")
    + pl.col("").list.get(1).str.contains("^Pushvarsel!.*")
    + pl.col("").list.get(1).str.contains("^Pushvarsel!.*")
    + pl.col("").list.get(1).str.contains("^Vis flere.*")
    + pl.col("").list.get(1).str.contains("^Mer om.*")
    + pl.col("").list.get(1).str.contains("^Foto:.*")
    + pl.col("").list.get(1).str.contains("^Les også.*")
    + pl.col("").list.get(1).str.contains("^LES OGSÅ.*")
    + pl.col("").list.get(1).str.contains("^Se også.*")
    + pl.col("").list.get(1).str.contains("^Javascript er skrudd av.*")
    + pl.col("")
    .list.get(1)
    .str.contains(
        "^Pga av dette vil deler av innholdet på dette nettstedet ikke vises.*"
    )
    + pl.col("").list.get(1).str.contains("^Artikkelen er flere år gammel.$")
    + pl.col("")
    .list.get(1)
    .str.contains("[a-zA-Z]eninger i teksten står for skribentens regning")
    + pl.col("").list.get(1).str.contains("Dette er et debattinnlegg")
    + pl.col("").list.get(1).str.contains("Dette er en kronikk")
    + pl.col("")
    .list.get(1)
    .str.contains(r"^13.21 år\? Vil du også skrive til Si .D\?.*")
    + pl.col("").list.get(1).str.contains(r"Skal du delta i kommentarfeltet")
    + pl.col("").list.get(1).str.contains(r"Publisert.+:")
    + pl.col("").list.get(1).str.contains(r"^Artikkelen er over \w+ år gammel.$")
    + pl.col("").list.get(1).str.contains(r"^Artikkelen er mer enn \w+ år gammel.$")
    + pl.col("").list.get(1).str.contains("^NTB.*")
    + pl.col("").list.get(1).str.contains(r"\(.NTB\)")
    + pl.col("").list.get(1).str.contains(r"Saken oppdateres")
    + pl.col("").list.get(1).str.contains(r"Kontakt. Pressetelefon.*norges\-bank.no")
    + pl.col("").list.get(0).str.contains("^figcaption$")
)

lf_nettavisen: pl.LazyFrame = pl.scan_parquet("G:/raw/nettavisen_raw.parquet").filter(
    ~pl.col("url").str.contains_any(
        [
            "/somali/",
            "/sport/",
            "/egenpromo/",
            "/russisk/",
            "/puls/",
            "/sportspill/",
            "/tjenester/",
            "/shoppingtips/",
            "/video/",
            "/arabisk/",
            "/polsk/",
            "/annonsebilag/",
        ]
    )
)

lf_nettavisen_add_columns: pl.LazyFrame = lf_nettavisen.with_columns(
    pl.col("html")
    .str.extract("(?s)<article.*>.*<h1.*>(.*)</h1>.*</article>")
    .str.strip_chars()
    .alias("title"),
    pl.col("html")
    .str.extract("(?s)<time.*datetime=.(.*).>.*</time>")
    .alias("datetime"),
    pl.col("html")
    .map_elements(
        get_article_content,
        return_dtype=pl.List(pl.List(pl.Utf8)),
        strategy="threading",
    )
    .alias("article_content"),
    site=pl.lit("Nettavisen"),
    is_bulletin=pl.lit(False),
)

lf_nettavisen_process_article_content: pl.LazyFrame = (
    lf_nettavisen_add_columns.with_columns(
        pl.col("article_content").map_batches(
            process_article_content,
            is_elementwise=True,
            return_dtype=pl.List(pl.List(pl.Utf8)),
        )
    )
)

lf_nettavisen_filter: pl.LazyFrame = lf_nettavisen_process_article_content.filter(
    (
        ~pl.col("article_content")
        .list.eval(pl.col("").list.get(1).str.contains("Annonsørinnhold").any())
        .list.first()
    ),
    pl.col("datetime").str.to_datetime().dt.year() <= 2023,
)

lf_nettavisen_finished: pl.LazyFrame = (
    lf_nettavisen_filter.drop_nulls()
    .drop("html")
    .select(["title", "article_content", "url", "datetime", "is_bulletin", "site"])
)

lf_nrk: pl.LazyFrame = (
    pl.scan_parquet("G:/raw/nrk_raw.parquet")
    .filter(
        pl.col("html")
        .str.extract(
            r"published_time. content=.([12]0[0129][0-9]-[01][0-9]-[0-3][0-9]T[012][0-9]:[0-6][0-9]:[0-6][0-9]\+[012][0-9]:["
            r"0-6][0-9])"
        )
        .str.to_datetime()
        .dt.year()
        >= 2018,
        pl.col("html")
        .str.extract(
            r"published_time. content=.([12]0[0129][0-9]-[01]["
            r"0-9]-[0-3][0-9]T[012][0-9]:[0-6][0-9]:[0-6][0-9]\+["
            r"012][0-9]:[0-6][0-9])"
        )
        .str.to_datetime()
        .dt.year()
        <= 2023,
    )
    .filter(
        pl.col("url").str.contains_any(
            [
                "/troms/",
                "/finmark/",
                "/utenriks/",
                "/buskerud/",
                "/dokumentar/",
                "/osloogviken/",
                "/vestlandet/",
                "/innlandet/",
                "/nordland/",
                "/debatten/",
                "/ostfold/",
                "/vestfoldogtelemark/",
                "/sorlandet/",
                "/viten/",
                "/urix/",
                "/nyheter/",
                "/ho/",
                "/spesial/",
                "/kultur/",
                "/mr/",
                "/klima/",
                "/rogaland/",
                "/norge/",
                "/vestfold/",
                "/korona/",
                "/sognogfjordane/",
                "/svalbard/",
                "/livsstil/",
                "/okonomi/",
                "/telemark/",
                "/tromsogfinmark/",
                "/trondelag/",
                "/ytring/",
                "/ostlandssendingen/",
            ]
        )
    )
)

lf_nrk_add_columns: pl.LazyFrame = (
    lf_nrk.with_columns(
        pl.col("html")
        .str.extract(
            r"published_time. content=.([12]0[0129][0-9]-[01][0-9]-[0-3][0-9]T[012][0-9]:[0-6][0-9]:[0-6][0-9]\+[012][0-9]:["
            r"0-6][0-9])"
        )
        .alias("datetime"),
        pl.col("html").str.extract(r"<h1.*>(.*)</h1>").alias("title"),
        pl.col("html").str.extract("bulletin-title.>(.*)</h2>").alias("bulletin_title"),
        site=pl.lit("NRK"),
    )
    .with_columns(
        pl.col("html")
        .map_elements(
            get_article_content,
            return_dtype=pl.List(pl.List(pl.Utf8)),
            strategy="threading",
        )
        .alias("article_content"),
        is_bulletin=pl.col("bulletin_title").is_not_null(),
    )
    .with_columns(
        pl.concat_list(["title", "bulletin_title"])
        .list.drop_nulls()
        .list.first()
        .alias("title")
    )
    .drop("bulletin_title")
)

lf_nrk_process_article_content: pl.LazyFrame = lf_nrk_add_columns.with_columns(
    pl.col("article_content").map_batches(
        process_article_content,
        is_elementwise=True,
        return_dtype=pl.List(pl.List(pl.Utf8)),
    )
)

lf_nrk_finished: pl.LazyFrame = lf_nrk_process_article_content.drop("html").select(
    ["title", "article_content", "url", "datetime", "is_bulletin", "site"]
)

lf_vg: pl.LazyFrame = pl.scan_parquet("G:/raw/vg_raw.parquet").filter(
    pl.col("url")
    .str.extract(r"(?U)\.no/(.*)/")
    .str.contains_any(["nyheter", "forbruker", "i"])
)

lf_vg_add_columns: pl.LazyFrame = lf_vg.with_columns(
    pl.col("html")
    .str.extract(
        r"([12]0[0129][0-9]-[01][0-9]-[0-3][0-9]T[012][0-9]:[0-6][0-9]:[0-6][0-9]Z?)"
    )
    .alias("datetime"),
    pl.col("html").str.extract(r"<h1.*>(.*)</h1>").alias("title"),
).with_columns(
    pl.col("html")
    .map_elements(
        get_article_content,
        return_dtype=pl.List(pl.List(pl.Utf8)),
        strategy="threading",
    )
    .alias("article_content"),
    site=pl.lit("VG"),
    is_bulletin=pl.lit(False),
)

lf_vg_process_article_content: pl.LazyFrame = lf_vg_add_columns.with_columns(
    pl.col("article_content").map_batches(
        process_article_content,
        is_elementwise=True,
        return_dtype=pl.List(pl.List(pl.Utf8)),
    )
)

lf_vg_filter: pl.LazyFrame = lf_vg_process_article_content.filter(
    (
        ~pl.col("article_content")
        .list.eval(pl.col("").list.get(1).str.contains(r"VG\+").any())
        .list.first()
    )
)

lf_vg_finished: pl.LazyFrame = lf_vg_filter.drop("html").select(
    ["title", "article_content", "url", "datetime", "is_bulletin", "site"]
)

lf_aftenposten: pl.LazyFrame = pl.scan_parquet("G:/raw/aftenposten_raw.parquet").filter(
    pl.col("url")
    .str.extract(r"(?U)\.no/(.*)/")
    .str.contains_any(["nyheter", "forbruker", "i"])
)

lf_aftenposten_add_columns: pl.LazyFrame = lf_aftenposten.with_columns(
    pl.col("html")
    .str.extract(
        r"([12]0[0129][0-9]-[01][0-9]-[0-3][0-9]T[012][0-9]:[0-6][0-9]:[0-6][0-9]Z?)"
    )
    .alias("datetime"),
    pl.col("html").str.extract(r"<h1.*>(.*)</h1>").alias("title"),
    site=pl.lit("Aftenposten"),
    is_bulletin=pl.lit(False),
).with_columns(
    pl.col("html")
    .map_elements(
        get_article_content,
        return_dtype=pl.List(pl.List(pl.Utf8)),
        strategy="threading",
    )
    .alias("article_content")
)

lf_aftenposten_process_article_content: pl.LazyFrame = (
    lf_aftenposten_add_columns.with_columns(
        pl.col("article_content").map_batches(
            process_article_content,
            is_elementwise=True,
            return_dtype=pl.List(pl.List(pl.Utf8)),
        )
    )
)

lf_aftenposten_finished: pl.LazyFrame = lf_aftenposten_process_article_content.drop(
    "html"
).select(["title", "article_content", "url", "datetime", "is_bulletin", "site"])

lf_norgesbank: pl.LazyFrame = pl.scan_parquet("G:/raw/norgesbank_raw.parquet").filter(
    pl.col("url").str.contains_any(
        [
            "https://www.norges-bank.no/aktuelt/nyheter-og-hendelser/pressemeldinger",
            "https://www.norges-bank.no/aktuelt/nyheter-og-hendelser/nyhetsmeldinger",
            "https://www.norges-bank.no/aktuelt/nyheter-og-hendelser/foredrag-og-taler",
            "https://www.norges-bank.no/aktuelt/nyheter-og-hendelser/brev-og-uttalelser",
            "https://www.norges-bank.no/aktuelt/nyheter-og-hendelser/rundskriv",
            "https://www.norges-bank.no/aktuelt/nyheter-og-hendelser/artikler-og-kronikker",
            "https://www.norges-bank.no/tema/finansiell-stabilitet",
            "https://www.norges-bank.no/tema/Forskning",
            "https://www.norges-bank.no/tema/pengepolitikk",
            "https://www.norges-bank.no/tema/Statistikk",
            "https://www.norges-bank.no/tema/markeder-likviditet",
            "https://www.norges-bank.no/bankplassen",
        ],
        ascii_case_insensitive=True,
    )
)

lf_norgesbank_add_columns: pl.LazyFrame = (
    lf_norgesbank.with_columns(
        pl.col("html")
        .str.extract(
            r"(?:<span class=\Wprop\W>Publisert</span> )([0-9]{1,2}. \w+ [1-2][0-9]{1,3})"
        )
        .str.replace_many(
            ["januar", "februar", "mars", "mai", "juni", "juli", "oktober", "desember"],
            [
                "january",
                "february",
                "march",
                "may",
                "june",
                "july",
                "october",
                "december",
            ],
        )
        .str.replace(r"(^\d\.)", "0${1}")
        .str.to_date("%d. %B %Y")
        .alias("date"),
        pl.col("html")
        .str.extract(
            r"(?:<span class=\Wprop\W>Publisert</span> )(?:[0-9]{1,"
            r"2}. \w+ [1-2][0-9]{1,3})(?: <span class=\Wtime\W>)(["
            r"0-2][0-9]:[0-5][0-9])"
        )
        .str.to_time("%H:%M")
        .alias("time"),
    )
    .with_columns(
        pl.col("date").dt.combine(pl.col("time")).alias("datetime").cast(pl.Utf8)
    )
    .drop(["date", "time"])
    .drop_nulls()
    .filter(pl.col("datetime").str.to_datetime().dt.year() >= 2018)
    .with_columns(
        pl.col("html").str.extract(r"<h1>(.*)</h1>").alias("title"),
        pl.col("html")
        .map_elements(
            get_article_content,
            return_dtype=pl.List(pl.List(pl.Utf8)),
            strategy="threading",
        )
        .alias("article_content"),
        site=pl.lit("Norges Bank"),
        is_bulletin=pl.lit(False),
    )
)

lf_norgesbank_process_article_content: pl.LazyFrame = (
    lf_norgesbank_add_columns.with_columns(
        pl.col("article_content").map_batches(
            process_article_content,
            is_elementwise=True,
            return_dtype=pl.List(pl.List(pl.Utf8)),
        )
    ).with_columns(
        pl.col("article_content").list.eval(
            pl.col("").filter(pl.col("").list.get(1).str.len_bytes() > 0)
        )
    )
)

lf_norgesbank_finished: pl.LazyFrame = lf_norgesbank_process_article_content.drop(
    "html"
).select(["title", "article_content", "url", "datetime", "is_bulletin", "site"])

# Writing Nettavisen
print("Writing Nettavisen")
lf_nettavisen_finished.sink_parquet("data/nettavisen.parquet")
# Writing NRK
print("Writing NRK")
lf_nrk_finished.sink_parquet("data/nrk.parquet")
# Writing VGl
print("Writing VG")
lf_vg_finished.sink_parquet("data/vg.parquet")
# Writing Aftenposten
print("Writing Aftenposten")
lf_aftenposten_finished.sink_parquet("data/aftenposten.parquet")
# Writing Norges Bank
print("Writing Norges Bank")
lf_norgesbank_finished.sink_parquet("data/norgesbank.parquet")


df = pl.read_parquet("../data/*.parquet")

(
    (
        df.with_columns(
            pl.col("article_content")
            .list.eval(pl.col("").list.get(1).str.concat(" "))
            .list.first()
            .alias("article_joined")
        )
        .filter(pl.col("article_joined").str.len_bytes() > 0)
        .with_columns(
            pl.col("datetime")
            .str.extract(r"(20[1-2][0-9]-[0-1][0-9]-[0-3][0-9])")
            .alias("date"),
            pl.col("datetime")
            .str.extract(r"([0-2][0-9]:[0-5][0-9]:[0-5][0-9])")
            .alias("time"),
            pl.when(pl.col("datetime").str.contains(r".*(Z)"))
            .then(pl.lit(r"+0000"))
            .when(pl.col("datetime").str.contains(r".*(\+0100)"))
            .then(pl.lit(r"+0100"))
            .when(pl.col("datetime").str.contains(r".*(\+0000)"))
            .then(pl.lit(r"+0000"))
            .when(pl.col("datetime").str.contains(r".*(\.000000)"))
            .then(pl.lit(r"+0100"))
            .alias("timezone"),
        )
        .with_columns(
            pl.concat_str([pl.col("date"), pl.col("time")], separator="T").alias(
                "datetime"
            )
        )
    )
    .with_columns(
        pl.concat_str([pl.col("datetime"), pl.col("timezone")], separator="")
        .str.to_datetime()
        .alias("datetime")
    )
    .drop("date", "time", "timezone")
    .with_columns(pl.col("url").str.extract(r"(?U)\.no/(.*)/").alias("category"))
    .filter(pl.col("datetime").dt.year() >= 2018, pl.col("datetime").dt.year() <= 2023)
    .write_parquet("../data/data_kombinert.parquet")
)

df = pl.read_parquet("../data/data_kombinert.parquet")
df.with_columns(
    pl.col("article_joined").map_elements(
        lambda s: sent_tokenize(s),
        return_dtype=pl.List(pl.Utf8),
        strategy="threading",
    )
).select(pl.col("article_joined").explode()).write_csv(
    "../data/sentences.csv", include_header=False
)
