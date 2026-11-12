"""Render v1 of the bilingual topic catalog for the canonical YapChat migration."""
from __future__ import annotations

import os
import sys

from benchmark_text import MODELS, model_files
from embedding import encode_batch
from service import EMBEDDING_ID


VERSION = 1
TOPICS = (
    ("animals", "Животные", "Animals", "Домашние животные, кошки, собаки и уход за питомцами.",
     "Pets, cats, dogs and caring for animals."),
    ("food", "Еда и напитки", "Food and drink", "Кулинария, домашние рецепты, кофе и чай, новые кафе.",
     "Cooking, home recipes, coffee and tea, and new places to eat."),
    ("nature", "Природа", "Nature", "Леса, горы, озёра, растения, походы и наблюдения за природой.",
     "Forests, mountains, lakes, plants, hiking and watching wildlife."),
    ("travel", "Путешествия", "Travel", "Путешествия, поездки в другие города и страны, маршруты и впечатления.",
     "Travel, city breaks, trips abroad, itineraries and travel stories."),
    ("sports", "Спорт и движение", "Sports and fitness", "Спорт, тренировки, бег, плавание и велосипед.",
     "Sports, exercise, running, swimming and cycling."),
    ("music", "Музыка", "Music", "Музыка, гитара, игра на инструментах, новые песни и концерты.",
     "Music, guitar, playing instruments, new songs and concerts."),
    ("film", "Кино и сериалы", "Film and TV", "Фильмы, кино, сериалы, анимация и обсуждение просмотров.",
     "Movies, television series, animation and conversations about what to watch."),
    ("books", "Книги", "Books", "Книги, художественная литература, чтение и книжные рекомендации.",
     "Books, fiction, reading and recommendations for what to read."),
    ("games", "Игры", "Games", "Видеоигры, настольные игры, шахматы, головоломки и игровые встречи.",
     "Video games, board games, chess, puzzles and playing together."),
    ("technology", "Технологии", "Technology", "Технологии, программирование, приложения, устройства и электроника.",
     "Technology, programming, software, gadgets and electronics."),
    ("art", "Искусство и творчество", "Art and creativity", "Рисование, фотография, дизайн, ремесло и выставки.",
     "Drawing, photography, design, crafts and art exhibitions."),
    ("city", "Городская жизнь", "City life", "Места в городе, локальные события, прогулки и новые знакомства.",
     "Places around town, local events, walks and meeting new people."),
)


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def render():
    os.environ.update(USE_TORCH="0", TOKENIZERS_PARALLELISM="false", TRANSFORMERS_VERBOSITY="error")
    import numpy as np
    import onnxruntime as ort
    from transformers import AutoTokenizer

    model = MODELS["e5-int8"]
    files = model_files(model, False)
    tokenizer = AutoTokenizer.from_pretrained(files["tokenizer.json"].parent, local_files_only=True)
    options = ort.SessionOptions()
    options.intra_op_num_threads = 2
    options.inter_op_num_threads = 1
    session = ort.InferenceSession(str(files[model.weights]), sess_options=options, providers=["CPUExecutionProvider"])
    rows = []
    for ordinal, (key, ru, en, ru_seed, en_seed) in enumerate(TOPICS, 1):
        bilingual = encode_batch(
            session, tokenizer, [ru_seed, en_seed], prefix=model.query_prefix,
            pooling=model.pooling, max_tokens=model.max_tokens,
        ).astype(np.float64)
        vector = bilingual.mean(axis=0)
        vector /= np.linalg.norm(vector)
        if vector.shape != (model.dimensions,) or not np.isfinite(vector).all():
            raise ValueError("invalid topic embedding")
        literal = "[" + ",".join(format(float(value), ".7f") for value in vector) + "]"
        rows.append("    (" + ", ".join([
            str(VERSION), sql_string(key), str(ordinal), sql_string(ru), sql_string(en),
            sql_string(EMBEDDING_ID), sql_string(literal) + "::extensions.vector",
        ]) + ")")
    return ("insert into recsys.interest_topics(catalog_version,id,ordinal,label_ru,label_en,space,embedding) values\n"
            + ",\n".join(rows) + ";\n")


if __name__ == "__main__":
    sys.stdout.buffer.write(render().encode("utf-8"))
