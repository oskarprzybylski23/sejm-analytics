# %%
import spacy
import re
import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt


# %%
# Load data and nlp model
df = pd.read_csv('../../data/statements.csv')
nlp = spacy.load("pl_core_news_sm")
df.head()

# %%
df.info(verbose=True)

# %%
# Data has some incorrect types, convert
dfn = df.convert_dtypes()
dfn.dtypes

# %%
# Remove unneeded columns
dfn = dfn[["unique_id", "speaker_name", "content_text"]]
dfn.head()

# %%
# See the most frequent speakers
speaker = dfn.groupby("speaker_name")
# speaker.describe().head()

speaker.count().sort_values(by="content_text", ascending=False).head()

# %%
# Get text string from selected row
dfn_row = dfn.iloc[[42]]
raw_text = dfn_row["content_text"].values[0]

# %%
# Clean text with Spacy
def process_text(text):
    """
    Initial text processing.
    Convert to lowercase, remove text in parentheses and lemmatize.
    """

    doc = remove_parentheses(text)

    doc = nlp(doc.lower())

    tokens = [
        token.lemma_
        for token in doc
        if (
            not token.is_stop
            and not token.is_punct
            and not token.is_space
            and token.pos_ in {"NOUN", "PROPN", "ADJ"}
        )
    ]

    cleaned_text = " ".join(tokens)

    return cleaned_text


def remove_parentheses(text):
    """
    Remove all text in parentheses (including parentheses).
    Also removes any leading/trailing whitespace that may remain.
    """
    cleaned = re.sub(r'\([^)]*\)', '', text)
    # Remove extra spaces that may result from removal
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()

# %%
# Generate a word cloud from text


def generate_word_cloud(text):
    """
    Generate a word cloud from text
    """
    wordcloud = WordCloud(
        width=2000,
        height=1000,
        background_color='black').generate(str(text))
    fig = plt.figure(
        figsize=(40, 30),
        facecolor='k',
        edgecolor='k')
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.tight_layout(pad=0)
    plt.show()


# %%
# Aggregate all statements from a speaker and generate a wordcloud
selected_speaker = "Jarosław Kaczyński"

speaker_rows = dfn[dfn["speaker_name"] == selected_speaker]
all_text = " ".join(speaker_rows["content_text"].dropna().values)
cleaned_text_all = process_text(all_text)
print(speaker_rows)
generate_word_cloud(cleaned_text_all)

# %%
