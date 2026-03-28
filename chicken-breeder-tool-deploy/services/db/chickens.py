from datetime import datetime, timezone
from services.db.connection import get_connection


def upsert_chicken(record: dict):
    now_utc = datetime.now(timezone.utc).isoformat()

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO chickens (
                token_id,
                wallet_address,
                contract_address,
                name,
                nickname,
                image,
                token_uri,

                raw_state,
                state,
                is_dead,
                is_egg,

                breeding_time,
                breeding_time_remaining,
                breed_count,

                type,
                gender,
                level,

                generation_text,
                generation_num,

                parent_1,
                parent_2,

                instinct,

                beak,
                comb,
                eyes,
                feet,
                wings,
                tail,
                body,

                beak_h1,
                beak_h2,
                beak_h3,
                comb_h1,
                comb_h2,
                comb_h3,
                eyes_h1,
                eyes_h2,
                eyes_h3,
                feet_h1,
                feet_h2,
                feet_h3,
                wings_h1,
                wings_h2,
                wings_h3,
                tail_h1,
                tail_h2,
                tail_h3,
                body_h1,
                body_h2,
                body_h3,

                gene_profile_loaded,
                gene_last_updated,

                primary_build,
                primary_build_match_count,
                primary_build_match_total,

                recessive_build,
                recessive_build_match_count,
                recessive_build_match_total,
                recessive_build_repeat_bonus,

                ultimate_type,

                innate_attack,
                innate_defense,
                innate_speed,
                innate_health,
                innate_ferocity,
                innate_cockrage,
                innate_evasion,

                ip,
                last_updated
            )
            VALUES (
                :token_id,
                :wallet_address,
                :contract_address,
                :name,
                :nickname,
                :image,
                :token_uri,

                :raw_state,
                :state,
                :is_dead,
                :is_egg,

                :breeding_time,
                :breeding_time_remaining,
                :breed_count,

                :type,
                :gender,
                :level,

                :generation_text,
                :generation_num,

                :parent_1,
                :parent_2,

                :instinct,

                :beak,
                :comb,
                :eyes,
                :feet,
                :wings,
                :tail,
                :body,

                :beak_h1,
                :beak_h2,
                :beak_h3,
                :comb_h1,
                :comb_h2,
                :comb_h3,
                :eyes_h1,
                :eyes_h2,
                :eyes_h3,
                :feet_h1,
                :feet_h2,
                :feet_h3,
                :wings_h1,
                :wings_h2,
                :wings_h3,
                :tail_h1,
                :tail_h2,
                :tail_h3,
                :body_h1,
                :body_h2,
                :body_h3,

                :gene_profile_loaded,
                :gene_last_updated,

                :primary_build,
                :primary_build_match_count,
                :primary_build_match_total,

                :recessive_build,
                :recessive_build_match_count,
                :recessive_build_match_total,
                :recessive_build_repeat_bonus,

                :ultimate_type,

                :innate_attack,
                :innate_defense,
                :innate_speed,
                :innate_health,
                :innate_ferocity,
                :innate_cockrage,
                :innate_evasion,

                :ip,
                :last_updated
            )
            ON CONFLICT(token_id) DO UPDATE SET
                wallet_address = COALESCE(excluded.wallet_address, chickens.wallet_address),
                contract_address = COALESCE(excluded.contract_address, chickens.contract_address),
                name = COALESCE(excluded.name, chickens.name),
                nickname = COALESCE(excluded.nickname, chickens.nickname),
                image = COALESCE(excluded.image, chickens.image),
                token_uri = COALESCE(excluded.token_uri, chickens.token_uri),

                raw_state = COALESCE(excluded.raw_state, chickens.raw_state),
                state = COALESCE(excluded.state, chickens.state),
                is_dead = COALESCE(excluded.is_dead, chickens.is_dead),
                is_egg = COALESCE(excluded.is_egg, chickens.is_egg),

                breeding_time = COALESCE(excluded.breeding_time, chickens.breeding_time),
                breeding_time_remaining = COALESCE(excluded.breeding_time_remaining, chickens.breeding_time_remaining),
                breed_count = COALESCE(excluded.breed_count, chickens.breed_count),

                type = COALESCE(excluded.type, chickens.type),
                gender = COALESCE(excluded.gender, chickens.gender),
                level = COALESCE(excluded.level, chickens.level),

                generation_text = COALESCE(excluded.generation_text, chickens.generation_text),
                generation_num = COALESCE(excluded.generation_num, chickens.generation_num),

                parent_1 = COALESCE(excluded.parent_1, chickens.parent_1),
                parent_2 = COALESCE(excluded.parent_2, chickens.parent_2),

                instinct = COALESCE(excluded.instinct, chickens.instinct),

                beak = COALESCE(excluded.beak, chickens.beak),
                comb = COALESCE(excluded.comb, chickens.comb),
                eyes = COALESCE(excluded.eyes, chickens.eyes),
                feet = COALESCE(excluded.feet, chickens.feet),
                wings = COALESCE(excluded.wings, chickens.wings),
                tail = COALESCE(excluded.tail, chickens.tail),
                body = COALESCE(excluded.body, chickens.body),

                beak_h1 = COALESCE(excluded.beak_h1, chickens.beak_h1),
                beak_h2 = COALESCE(excluded.beak_h2, chickens.beak_h2),
                beak_h3 = COALESCE(excluded.beak_h3, chickens.beak_h3),
                comb_h1 = COALESCE(excluded.comb_h1, chickens.comb_h1),
                comb_h2 = COALESCE(excluded.comb_h2, chickens.comb_h2),
                comb_h3 = COALESCE(excluded.comb_h3, chickens.comb_h3),
                eyes_h1 = COALESCE(excluded.eyes_h1, chickens.eyes_h1),
                eyes_h2 = COALESCE(excluded.eyes_h2, chickens.eyes_h2),
                eyes_h3 = COALESCE(excluded.eyes_h3, chickens.eyes_h3),
                feet_h1 = COALESCE(excluded.feet_h1, chickens.feet_h1),
                feet_h2 = COALESCE(excluded.feet_h2, chickens.feet_h2),
                feet_h3 = COALESCE(excluded.feet_h3, chickens.feet_h3),
                wings_h1 = COALESCE(excluded.wings_h1, chickens.wings_h1),
                wings_h2 = COALESCE(excluded.wings_h2, chickens.wings_h2),
                wings_h3 = COALESCE(excluded.wings_h3, chickens.wings_h3),
                tail_h1 = COALESCE(excluded.tail_h1, chickens.tail_h1),
                tail_h2 = COALESCE(excluded.tail_h2, chickens.tail_h2),
                tail_h3 = COALESCE(excluded.tail_h3, chickens.tail_h3),
                body_h1 = COALESCE(excluded.body_h1, chickens.body_h1),
                body_h2 = COALESCE(excluded.body_h2, chickens.body_h2),
                body_h3 = COALESCE(excluded.body_h3, chickens.body_h3),

                gene_profile_loaded = COALESCE(excluded.gene_profile_loaded, chickens.gene_profile_loaded),
                gene_last_updated = COALESCE(excluded.gene_last_updated, chickens.gene_last_updated),

                primary_build = excluded.primary_build,
                primary_build_match_count = COALESCE(excluded.primary_build_match_count, chickens.primary_build_match_count),
                primary_build_match_total = COALESCE(excluded.primary_build_match_total, chickens.primary_build_match_total),

                recessive_build = excluded.recessive_build,
                recessive_build_match_count = COALESCE(excluded.recessive_build_match_count, chickens.recessive_build_match_count),
                recessive_build_match_total = COALESCE(excluded.recessive_build_match_total, chickens.recessive_build_match_total),
                recessive_build_repeat_bonus = COALESCE(excluded.recessive_build_repeat_bonus, chickens.recessive_build_repeat_bonus),

                ultimate_type = excluded.ultimate_type,

                innate_attack = COALESCE(excluded.innate_attack, chickens.innate_attack),
                innate_defense = COALESCE(excluded.innate_defense, chickens.innate_defense),
                innate_speed = COALESCE(excluded.innate_speed, chickens.innate_speed),
                innate_health = COALESCE(excluded.innate_health, chickens.innate_health),
                innate_ferocity = COALESCE(excluded.innate_ferocity, chickens.innate_ferocity),
                innate_cockrage = COALESCE(excluded.innate_cockrage, chickens.innate_cockrage),
                innate_evasion = COALESCE(excluded.innate_evasion, chickens.innate_evasion),

                ip = COALESCE(excluded.ip, chickens.ip),
                last_updated = excluded.last_updated
            """,
            {
                "token_id": str(record.get("token_id") or ""),
                "wallet_address": record.get("wallet_address"),
                "contract_address": record.get("contract_address"),
                "name": record.get("name"),
                "nickname": record.get("nickname"),
                "image": record.get("image"),
                "token_uri": record.get("token_uri"),

                "raw_state": record.get("raw_state"),
                "state": record.get("state"),
                "is_dead": 1 if record.get("is_dead") else 0 if record.get("is_dead") is not None else None,
                "is_egg": 1 if record.get("is_egg") else 0 if record.get("is_egg") is not None else None,

                "breeding_time": record.get("breeding_time"),
                "breeding_time_remaining": record.get("breeding_time_remaining"),
                "breed_count": record.get("breed_count"),

                "type": record.get("type"),
                "gender": record.get("gender"),
                "level": record.get("level"),

                "generation_text": record.get("generation_text"),
                "generation_num": record.get("generation_num"),

                "parent_1": record.get("parent_1"),
                "parent_2": record.get("parent_2"),

                "instinct": record.get("instinct"),

                "beak": record.get("beak"),
                "comb": record.get("comb"),
                "eyes": record.get("eyes"),
                "feet": record.get("feet"),
                "wings": record.get("wings"),
                "tail": record.get("tail"),
                "body": record.get("body"),

                "beak_h1": record.get("beak_h1"),
                "beak_h2": record.get("beak_h2"),
                "beak_h3": record.get("beak_h3"),
                "comb_h1": record.get("comb_h1"),
                "comb_h2": record.get("comb_h2"),
                "comb_h3": record.get("comb_h3"),
                "eyes_h1": record.get("eyes_h1"),
                "eyes_h2": record.get("eyes_h2"),
                "eyes_h3": record.get("eyes_h3"),
                "feet_h1": record.get("feet_h1"),
                "feet_h2": record.get("feet_h2"),
                "feet_h3": record.get("feet_h3"),
                "wings_h1": record.get("wings_h1"),
                "wings_h2": record.get("wings_h2"),
                "wings_h3": record.get("wings_h3"),
                "tail_h1": record.get("tail_h1"),
                "tail_h2": record.get("tail_h2"),
                "tail_h3": record.get("tail_h3"),
                "body_h1": record.get("body_h1"),
                "body_h2": record.get("body_h2"),
                "body_h3": record.get("body_h3"),

                "gene_profile_loaded": record.get("gene_profile_loaded"),
                "gene_last_updated": record.get("gene_last_updated"),

                "primary_build": record.get("primary_build"),
                "primary_build_match_count": record.get("primary_build_match_count"),
                "primary_build_match_total": record.get("primary_build_match_total"),

                "recessive_build": record.get("recessive_build"),
                "recessive_build_match_count": record.get("recessive_build_match_count"),
                "recessive_build_match_total": record.get("recessive_build_match_total"),
                "recessive_build_repeat_bonus": record.get("recessive_build_repeat_bonus"),

                "ultimate_type": record.get("ultimate_type"),

                "innate_attack": record.get("innate_attack"),
                "innate_defense": record.get("innate_defense"),
                "innate_speed": record.get("innate_speed"),
                "innate_health": record.get("innate_health"),
                "innate_ferocity": record.get("innate_ferocity"),
                "innate_cockrage": record.get("innate_cockrage"),
                "innate_evasion": record.get("innate_evasion"),

                "ip": record.get("ip"),
                "last_updated": now_utc,
            },
        )
        conn.commit()


def get_chicken_by_token(token_id: str):
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM chickens
            WHERE token_id = ?
            """,
            (str(token_id),),
        ).fetchone()

    return dict(row) if row else None


def get_chickens_by_wallet(wallet_address: str):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                c.*,
                COALESCE(fr.owned_root_count, 0) AS owned_root_count,
                COALESCE(fr.total_root_count, 0) AS total_root_count,
                COALESCE(fr.ownership_percent, 0) AS ownership_percent,
                COALESCE(fr.is_complete, 0) AS is_complete
            FROM chickens c
            LEFT JOIN chicken_family_roots fr
                ON fr.wallet_address = ?
               AND fr.token_id = c.token_id
            WHERE c.wallet_address = ?
            ORDER BY CAST(c.token_id AS INTEGER)
            """,
            (wallet_address, wallet_address),
        ).fetchall()

    result = []

    for row in rows:
        item = dict(row)

        generation_num = item.get("generation_num")
        generation_text = str(item.get("generation_text") or "").strip().lower()

        if generation_num == 0 or generation_text == "gen 0":
            item["owned_root_count"] = 1
            item["total_root_count"] = 1
            item["ownership_percent"] = 100.0
            item["is_complete"] = 1

        result.append(item)

    return result
