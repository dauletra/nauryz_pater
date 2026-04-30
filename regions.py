# Все регионы Казахстана с GUID из API baspana.otbasybank.kz
# Источник: HTML страницы /pool/search (Vue.js dropdown)

REGIONS: dict[str, str] = {
    "d1583e83-c699-4086-8166-ee3573482c22": "г. Астана",
    "f71a607b-48d8-4d1a-998f-f5bf1d3fd1e6": "г. Алматы",
    "2acbd946-7027-4f6f-a4d2-09985767fa67": "г. Шымкент",
    "01abccc5-66db-4dfd-ac13-ad7be5e08240": "Абайская обл.",
    "2ef6c753-d327-4049-a245-c3c4596f1bc6": "Акмолинская обл.",
    "4cc4c52f-fbf5-425b-acb5-1f2d354fa76e": "Актюбинская обл.",
    "28c55ae1-a345-42c9-b727-bce0a91e378d": "Алматинская обл.",
    "8938b4d8-1455-4c61-bb0f-7886288d2f71": "Атырауская обл.",
    "d1f6e7d7-e524-4c53-9427-8b0c9ab05ee2": "Восточно-Казахстанская обл.",
    "e58209b0-c1a5-4b11-8a5f-9f62b1320f75": "Жамбылская обл.",
    "a07b5aef-5e7b-4979-99f4-fcc2aa0889ab": "Жетысуская обл.",
    "089a41d2-95cd-46d3-8629-313074be9983": "Западно-Казахстанская обл.",
    "878a3ffb-7755-40d7-9b06-cba4c4390066": "Карагандинская обл.",
    "b612bb49-30e3-4380-a5c4-596ef57a65c2": "Костанайская обл.",
    "ad7ea7f2-6ff2-42a9-aa51-f44cec8849d6": "Кызылординская обл.",
    "0de7a4ab-e102-420d-b83e-0aff6e813dbf": "Мангистауская обл.",
    "e540010f-e94b-4f98-b4f3-01c9d219da55": "Павлодарская обл.",
    "7ba90c39-bec5-4609-84bf-deeef99212b1": "Северо-Казахстанская обл.",
    "e0bfb805-ea9d-49a5-a54e-239fd29125c2": "Туркестанская обл.",
    "ff524baf-175a-48f7-b76a-f1d07cbdef51": "Улытауская обл.",
}


def get_region_name(guid: str) -> str:
    """Вернуть название региона по GUID. Если не найден — вернуть GUID."""
    return REGIONS.get(guid, guid)


def get_all_regions() -> list[tuple[str, str]]:
    """Вернуть список всех регионов как [(guid, name), ...]."""
    return list(REGIONS.items())


def is_valid_region(guid: str) -> bool:
    return guid in REGIONS
