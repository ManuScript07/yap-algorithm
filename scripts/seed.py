"""Deterministic synthetic RU/EN corpus in the isolated Docker fixture only."""
import argparse
import json
import math
from pathlib import Path
import random
import subprocess
import uuid

TOPICS = {
    "programming": [("ru", "Перевёл небольшой сервис на Go. Разбираюсь с горутинами, PostgreSQL и обработкой ошибок."), ("en", "Building a small Go service. Learning concurrency, PostgreSQL and error handling.")],
    "music": [("ru", "Ищу компанию на концерт. Люблю живую музыку, джаз и небольшие музыкальные клубы."), ("en", "Looking for friends to go to a concert. I love live music, jazz and small venues.")],
    "sport": [("ru", "Вечерняя пробежка и тренировка. Обсуждаем бег, футбол и восстановление после спорта."), ("en", "Evening run and training. Talking about running, football and recovery after exercise.")],
    "games": [("ru", "Собираем команду для совместной игры. Обсуждаем игровые механики и любимые видеоигры."), ("en", "Putting together a team for cooperative games. Discussing game design and favorite video games.")],
    "cooking": [("ru", "Готовлю домашнюю пасту. Делюсь рецептом, выбираю специи и пробую новые блюда."), ("en", "Making homemade pasta. Sharing recipes, choosing spices and trying new dishes.")],
    "travel": [("ru", "Планирую поход выходного дня. Хочу найти попутчиков, красивый маршрут и место для палатки."), ("en", "Planning a weekend hike. Looking for travel companions, scenic trails and a camping spot.")],
    "science": [("ru", "Прочитал статью об астрономии. Интересно обсудить космос, телескопы и научные открытия."), ("en", "Read an astronomy paper. Would love to discuss space, telescopes and scientific discoveries.")],
    "art": [("ru", "Сегодня рисую городской пейзаж. Делюсь набросками и ищу компанию на выставку искусства."), ("en", "Painting a city landscape today. Sharing sketches and looking for company to visit an art exhibition.")],
}


def sql(text):
    return "'" + str(text).replace("'", "''") + "'"


def psql(source):
    result = subprocess.run(["docker", "exec", "-i", "yap_algorithm_db", "psql", "-U", "supabase_admin", "-d", "yap_algorithm", "-qAt", "-v", "ON_ERROR_STOP=1"], input=source, capture_output=True, text=True, encoding="utf-8", check=True)
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--posts", type=int, default=1000)
    parser.add_argument("--users", type=int, default=32)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if not 8 <= args.users <= 1000 or not 1 <= args.posts <= 200000:
        raise ValueError("unsupported fixture size")
    if psql("select current_database()='yap_algorithm'and recsys.is_fixture();") != "t":
        raise RuntimeError("Refusing non-fixture database")
    rng = random.Random(args.seed)
    ident = lambda x: str(uuid.uuid5(uuid.NAMESPACE_URL, f"yap-algorithm/{args.seed}/{x}"))
    topics = list(TOPICS)
    users = [{"id": ident(f"user/{i}"), "interests": rng.sample(topics, 2)} for i in range(args.users)]
    queries = []
    for i, user in enumerate(users):
        uid = sql(user["id"])
        queries.append(f"insert into public.profiles(id)values({uid})on conflict do nothing;")
        queries.append(f"insert into public.user_locations values({uid},55.75,37.61,now())on conflict(user_id)do update set updated_at=now();")
        queries.append(f"insert into private.search_privacy_settings values({uid},false)on conflict do nothing;")
    psql("\n".join(queries))
    queries = []
    for i, user in enumerate(users):
        uid = sql(user["id"])
        friend = sql(users[(i + 1) % len(users)]["id"])
        queries.append(f"insert into public.friendships values(least({uid}::uuid,{friend}::uuid),greatest({uid}::uuid,{friend}::uuid))on conflict do nothing;")
        for delta in (2, 3):
            author = sql(users[(i + delta) % len(users)]["id"])
            queries.append(f"insert into feed.follows(user_id,author_id)values({uid},{author})on conflict do nothing;")
    psql("\n".join(queries))
    posts = []
    rows = []
    for i in range(args.posts):
        author = users[rng.randrange(len(users))]["id"]
        topic = topics[rng.randrange(len(topics))]
        language, text = rng.choice(TOPICS[topic])
        # Authored synthetic text; topic IDs remain only in evaluation metadata.
        text += f" #{i % 97 + 1}"
        pid = ident(f"post/{i}")
        audience = "friends" if rng.random() < 0.2 else "public"
        latitude = 55.75 + rng.uniform(-0.8, 0.8)
        longitude = 37.61 + rng.uniform(-1.0, 1.0)
        age_minutes = rng.randrange(7 * 24 * 60)
        rows.append(f"({sql(pid)},{sql(author)},{sql(text)},{sql(audience)},{sql(language)},extensions.st_setsrid(extensions.st_makepoint({longitude},{latitude}),4326)::extensions.geography,now()-interval '{age_minutes} minutes')")
        posts.append({"id": pid, "topic": topic, "author_id": author})
        if len(rows) == 500 or i + 1 == args.posts:
            psql("insert into feed.posts(id,author_id,text,audience,language,location,created_at)values" + ",".join(rows) + "on conflict(id)do nothing;")
            rows = []
    Path(".local").mkdir(exist_ok=True)
    Path(".local/corpus.json").write_text(json.dumps({"origin": "synthetic", "seed": args.seed, "users": users, "posts": posts}, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"users": len(users), "posts": len(posts), "manifest": ".local/corpus.json", "origin": "synthetic"}))


if __name__ == "__main__":
    main()
