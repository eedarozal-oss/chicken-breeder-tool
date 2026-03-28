def to_int(value):
    if value is None or value == "":
        return None
    return int(value)


def get_stat(attributes: dict, primary: str, fallback: str = None):
    value = to_int(attributes.get(primary))

    if value is None and fallback:
        value = to_int(attributes.get(fallback))

    return value if value is not None else 0


def calculate_ip(attributes: dict, is_egg: bool, is_dead: bool):
    if is_egg or is_dead:
        return None

    innate_attack = get_stat(attributes, "Innate Attack")
    innate_defense = get_stat(attributes, "Innate Defense")
    innate_speed = get_stat(attributes, "Innate Speed")
    innate_health = get_stat(attributes, "Innate Health")

    innate_ferocity = get_stat(attributes, "Innate Ferocity", "Innate Defense")
    innate_cockrage = get_stat(attributes, "Innate Cockrage", "Innate Attack")
    innate_evasion = get_stat(attributes, "Innate Evasion", "Innate Speed")

    total = (
        innate_attack
        + innate_defense
        + innate_speed
        + innate_health
        + innate_ferocity
        + innate_cockrage
        + innate_evasion
    )

    return total
