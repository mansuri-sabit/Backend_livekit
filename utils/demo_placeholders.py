"""
Demo panel: substitute {{name}}, {{company}} (and {name}, {company}, [Name], [Company]) in greeting/script
with values from AI Script section. Used by outbound (log) and pipeline (TTS greeting + context).
"""

import re


def substitute_demo_placeholders(text: str, name: str | None = None, company: str | None = None) -> str:
    """
    Replace {{name}}, {{company}}, {name}, {company}, [Name], [Company] in text with provided values.
    Handles all placeholder formats used across static templates and MongoDB agent greetings.
    If name/company are None or empty, bracket-style placeholders are left as-is so the
    call does not literally say "name" or "company".
    """
    if not text or not isinstance(text, str):
        return text or ""
    name_val = (name or "").strip()
    company_val = (company or "").strip()
    result = text
    for placeholder, value in (
        ("{{name}}", name_val or "[Name]"),
        ("{{company}}", company_val or "[Company]"),
        ("{name}", name_val or "[Name]"),
        ("{company}", company_val or "[Company]"),
    ):
        result = result.replace(placeholder, value)
    if name_val:
        result = result.replace("[Name]", name_val)
    if company_val:
        result = result.replace("[Company]", company_val)
    return result


def parse_name_company_from_greeting(greeting: str) -> tuple[str | None, str | None]:
    """
    Fallback: extract name and company from greeting when not sent in the request.
    Handles patterns like "I'm Rajesh Sharma from Sunrise Properties" or "Hi, I'm X from Y".
    Returns (name, company) or (None, None) if not found.
    """
    if not greeting or not isinstance(greeting, str) or len(greeting) < 10:
        return None, None
    m = re.search(r"\bI\s*'?m\s+(.+?)\s+from\s+(.+?)(?:\.|,|\.\s|$)", greeting, re.IGNORECASE | re.DOTALL)
    if m:
        name_part = (m.group(1) or "").strip()
        company_part = (m.group(2) or "").strip()
        for sep in (".", ",", " I ", " I've", " I'll", " We ", " —"):
            if sep in name_part:
                name_part = name_part.split(sep)[0].strip()
            if sep in company_part:
                company_part = company_part.split(sep)[0].strip()
        if name_part and company_part and len(name_part) < 100 and len(company_part) < 100:
            return name_part, company_part
    return None, None
