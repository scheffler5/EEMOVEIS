import re


def _normalize_numeric_string(raw_value: str) -> str:
    cleaned = re.sub(r"[^\d,\.]", "", raw_value)
    if not cleaned:
        return ""

    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")

    return cleaned


def parse_price(price_str: str) -> float:
    """Converte 'R$ 550.000,00' em 550000.0."""
    if not price_str:
        return 0.0

    cleaned = _normalize_numeric_string(price_str)
    return float(cleaned) if cleaned else 0.0


def parse_area(area_str: str) -> float:
    """Converte '120 m²' em 120.0."""
    if not area_str:
        return 0.0

    cleaned = _normalize_numeric_string(area_str)
    return float(cleaned) if cleaned else 0.0