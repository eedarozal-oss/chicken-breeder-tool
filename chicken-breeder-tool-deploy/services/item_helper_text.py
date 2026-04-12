ITEM_NAME_ALIASES = {
    "Cocktail's Beak": "Cocktail's Obsidian Beak",
}

ITEM_HELPER_TEXT = {
    "Soulknot": "Guarantees inheritance of 3 out of 7 random Innate Points from this parent.",
    "Gregor's Gift": "Increases this parent's Primary Gene influence by 50%.",
    "Mendel's Memento": "Increases this parent's recessive Genes inheritance chance by 50%.",
    "Quentin's Talon": "Increases this parent's Feet Genes inheritance chance by 50%.",
    "Dragon's Whip": "Increases this parent's Tail Genes inheritance chance by 50%.",
    "Chibidei's Curse": "Increases this parent's Body Genes inheritance chance by 50%.",
    "All-seeing Seed": "Increases this parent's Eyes Genes inheritance chance by 50%.",
    "Chim Lac's Curio": "Increases this parent's Beak Genes inheritance chance by 50%.",
    "Suave Scissors": "Increases this parent's Comb Genes inheritance chance by 50%.",
    "Simurgh's Sovereign": "Increases this parent's Wings Genes inheritance chance by 50%.",
    "St. Elmo's Fire": "Increases this parent's Instinct inheritance chance by 50%.",
    "Cocktail's Obsidian Beak": "Increases this parent's Attack IP Genes inheritance chance by 50%.",
    "Pos2 Pellet": "Increases this parent's Defense IP Genes inheritance chance by 50%.",
    "Fetzzz Feet": "Increases this parent's Speed IP Genes inheritance chance by 50%.",
    "Vananderen's Vitality": "Increases this parent's Health IP Genes inheritance chance by 50%.",
    "Pinong's Bird": "Increases this parent's Cockrage IP Genes inheritance chance by 50%.",
    "Ouchie's Ornament": "Increases this parent's Ferocity IP Genes inheritance chance by 50%.",
    "Lockedin State": "Increases this parent's Evasion IP Genes inheritance chance by 50%.",
}


def normalize_item_name(item_name):
    raw = str(item_name or "").strip()
    return ITEM_NAME_ALIASES.get(raw, raw)


def get_item_helper_text(item_name):
    normalized = normalize_item_name(item_name)
    return ITEM_HELPER_TEXT.get(normalized, "")
