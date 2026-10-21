"""Urdu/Arabic-script text -> English letters, for customer names and addresses.

Urdu is written without short vowels, so a letter-by-letter conversion gives only a
rough spelling. Common names, places and address words are looked up first (WORDS),
everything else falls back to a letter map with a vowel inserted between consonants
so the result stays readable for the courier.
"""
import re

SCRIPT = re.compile(r"[؀-ۿݐ-ݿࢠ-ࣿﭐ-﷿ﹰ-﻿]")

WORDS = {
    "محمد": "Muhammad", "مُحَمَّد": "Muhammad", "احمد": "Ahmed", "علی": "Ali", "حسن": "Hassan",
    "حسین": "Hussain", "عمر": "Umar", "عثمان": "Usman", "بلال": "Bilal", "عباس": "Abbas",
    "اقبال": "Iqbal", "اشرف": "Ashraf", "اکبر": "Akbar", "ارشد": "Arshad", "اسلم": "Aslam",
    "امجد": "Amjad", "اعجاز": "Ejaz", "افضل": "Afzal", "ایوب": "Ayub", "جاوید": "Javed",
    "خان": "Khan", "شاہ": "Shah", "ملک": "Malik", "چوہدری": "Chaudhry", "رانا": "Rana",
    "بٹ": "Butt", "سید": "Syed", "حاجی": "Haji", "ڈاکٹر": "Doctor", "عبدالرحمن": "Abdul Rehman",
    "عبد": "Abdul", "الله": "Allah", "اللہ": "Allah", "بی بی": "Bibi", "بی": "Bi",
    "فاطمہ": "Fatima", "عائشہ": "Ayesha", "زینب": "Zainab", "خدیجہ": "Khadija",
    "لاہور": "Lahore", "کراچی": "Karachi", "ملتان": "Multan", "اسلام آباد": "Islamabad",
    "راولپنڈی": "Rawalpindi", "فیصل آباد": "Faisalabad", "پشاور": "Peshawar", "کوئٹہ": "Quetta",
    "حیدرآباد": "Hyderabad", "گوجرانوالہ": "Gujranwala", "سیالکوٹ": "Sialkot", "سرگودھا": "Sargodha",
    "بہاولپور": "Bahawalpur", "سکھر": "Sukkur", "جہلم": "Jhelum", "قصور": "Kasur", "ساہیوال": "Sahiwal",
    "گجرات": "Gujrat", "اوکاڑہ": "Okara", "میانوالی": "Mianwali", "مظفرگڑھ": "Muzaffargarh",
    "پنجاب": "Punjab", "سندھ": "Sindh", "بلوچستان": "Balochistan", "خیبر پختونخوا": "Khyber Pakhtunkhwa",
    "پاکستان": "Pakistan", "ضلع": "District", "تحصیل": "Tehsil", "شہر": "City", "گاؤں": "Village",
    "گاوں": "Village", "محلہ": "Mohalla", "گلی": "Street", "سڑک": "Road", "روڈ": "Road",
    "مکان": "House", "گھر": "House", "نمبر": "No", "نمبر.": "No", "بلاک": "Block", "سیکٹر": "Sector",
    "فیز": "Phase", "کالونی": "Colony", "ٹاؤن": "Town", "سوسائٹی": "Society", "چوک": "Chowk",
    "بازار": "Bazar", "مسجد": "Masjid", "اسکول": "School", "ہسپتال": "Hospital", "دکان": "Shop",
    "قریب": "Near", "سامنے": "Opposite", "پاس": "Near", "منزل": "Floor", "فلیٹ": "Flat",
    "پوسٹ آفس": "Post Office", "ڈاکخانہ": "Post Office", "اڈا": "Adda", "موڑ": "Mor",
}

LETTERS = {
    "ا": "a", "آ": "aa", "أ": "a", "إ": "i", "ب": "b", "پ": "p", "ت": "t", "ٹ": "t", "ث": "s",
    "ج": "j", "چ": "ch", "ح": "h", "خ": "kh", "د": "d", "ڈ": "d", "ذ": "z", "ر": "r", "ڑ": "r",
    "ز": "z", "ژ": "zh", "س": "s", "ش": "sh", "ص": "s", "ض": "z", "ط": "t", "ظ": "z", "ع": "a",
    "غ": "gh", "ف": "f", "ق": "q", "ک": "k", "ك": "k", "گ": "g", "ل": "l", "م": "m", "ن": "n",
    "ں": "n", "و": "o", "ؤ": "o", "ہ": "h", "ھ": "h", "ە": "h", "ۃ": "h", "ة": "h", "ء": "",
    "ی": "i", "ي": "i", "ئ": "i", "ے": "e", "ې": "e", "ۓ": "e",
    "،": ",", "؟": "?", "؛": ";", "۔": ".", "٪": "%", "٫": ".",
}
DIGITS = {ord(c): str(i) for i, c in enumerate("۰۱۲۳۴۵۶۷۸۹")}
DIGITS.update({ord(c): str(i) for i, c in enumerate("٠١٢٣٤٥٦٧٨٩")})
VOWELS = set("aeiou")
DIACRITICS = re.compile(r"[ً-ٰٟۖ-ۭ‌‍‎‏]")


def has_urdu(text) -> bool:
    return bool(text) and bool(SCRIPT.search(str(text)))


def _word(word: str) -> str:
    out = []
    for ch in word:
        out.append(LETTERS.get(ch, ch))
    # put an 'a' between consecutive consonants so it can be read: mhmd -> mahamad
    spelled = ""
    for i, piece in enumerate(out):
        spelled += piece
        nxt = out[i + 1] if i + 1 < len(out) else ""
        if (piece and nxt and piece[-1].isalpha() and nxt[0].isalpha()
                and piece[-1] not in VOWELS and nxt[0] not in VOWELS and nxt not in ("h",)):
            spelled += "a"
    return spelled.capitalize()


def to_english(text) -> str:
    """Replace Urdu/Arabic-script text by English letters, leave everything else alone."""
    if not has_urdu(text):
        return text
    text = DIACRITICS.sub("", str(text)).translate(DIGITS)
    for urdu in sorted(WORDS, key=len, reverse=True):  # longest phrase first, whole words only
        if urdu in text:
            text = re.sub(r"(?<![؀-ۿݐ-ݿﭐ-﷿ﹰ-﻿])"
                          + re.escape(urdu) +
                          r"(?![؀-ۿݐ-ݿﭐ-﷿ﹰ-﻿])",
                          " " + WORDS[urdu] + " ", text)
    parts = re.split(r"(\s+)", text)
    done = [(_word(p) if SCRIPT.search(p) else p) for p in parts]
    return re.sub(r"\s+", " ", "".join(done)).strip()
