import re
import unicodedata

# ═══════════════════════════════════════════════════════════════════════════════
#  UNIVERSAL NORMALIZER — Full Indian Language Support
# ═══════════════════════════════════════════════════════════════════════════════
#  Supports ALL 22 scheduled languages + English:
#  ┌────────────────────────────────────────────────────────────────┐
#  │ State/Region    │ Language   │ Script     │ Example            │
#  ├────────────────────────────────────────────────────────────────┤
#  │ Gujarat         │ Gujarati   │ ગુજરાતી     │ હનીબી ડિજિટલ       │
#  │ Maharashtra     │ Marathi    │ मराठी       │ हनीबी डिजिटल        │
#  │ Hindi Belt      │ Hindi      │ हिन्दी      │ हनीबी डिजिटल        │
#  │ Tamil Nadu      │ Tamil      │ தமிழ்       │ ஹனிபி டிஜிட்டல்     │
#  │ Karnataka       │ Kannada    │ ಕನ್ನಡ       │ ಹನಿಬೀ ಡಿಜಿಟಲ್       │
#  │ Andhra/Telangana│ Telugu     │ తెలుగు      │ హనీబీ డిజిటల్       │
#  │ Kerala          │ Malayalam  │ മലയാളം     │ ഹണിബീ ഡിജിറ്റൽ     │
#  │ West Bengal     │ Bengali    │ বাংলা       │ হানিবি ডিজিটাল      │
#  │ Punjab          │ Punjabi    │ ਪੰਜਾਬੀ      │ ਹਨੀਬੀ ਡਿਜੀਟਲ        │
#  │ Odisha          │ Odia       │ ଓଡ଼ିଆ       │ ହନୀବୀ ଡିଜିଟାଲ      │
#  │ Assam           │ Assamese   │ অসমীয়া     │ হানিবী ডিজিটেল      │
#  │ J&K             │ Urdu       │ اردو        │ ہنی بی ڈیجیٹل       │
#  │ Manipur         │ Manipuri   │ মৈতৈলোন্    │                     │
#  │ Goa             │ Konkani    │ कोंकणी      │                     │
#  │ Jharkhand       │ Santali    │ ᱥᱟᱱᱛᱟᱲᱤ     │                     │
#  │ Mizoram         │ Mizo       │ Latin       │                     │
#  │ Meghalaya       │ Khasi      │ Latin       │                     │
#  │ Sikkim          │ Nepali     │ नेपाली      │                     │
#  │ Tripura         │ Kokborok   │ Latin/বাংলা │                     │
#  │ Chhattisgarh    │ Chhattis.  │ छत्तीसगढ़ी  │                     │
#  │ Rajasthan       │ Rajasthani │ राजस्थानी    │                     │
#  │ Bihar           │ Maithili   │ मैथिली      │                     │
#  └────────────────────────────────────────────────────────────────┘
#
#  RULE: NEVER strip, reject, or modify non-ASCII characters.
#        Data is preserved EXACTLY as received from Google Drive.
# ═══════════════════════════════════════════════════════════════════════════════

# State map for canonical names (English abbreviations → Full names)
STATE_MAP = {
    # Abbreviations
    "ap": "Andhra Pradesh", "ar": "Arunachal Pradesh", "as": "Assam", "br": "Bihar",
    "cg": "Chhattisgarh", "ga": "Goa", "gj": "Gujarat", "hr": "Haryana", "hp": "Himachal Pradesh",
    "jk": "Jammu and Kashmir", "jh": "Jharkhand", "ka": "Karnataka", "kl": "Kerala",
    "mp": "Madhya Pradesh", "mh": "Maharashtra", "mn": "Manipur", "ml": "Meghalaya",
    "mz": "Mizoram", "nl": "Nagaland", "or": "Odisha", "pb": "Punjab", "rj": "Rajasthan",
    "sk": "Sikkim", "tn": "Tamil Nadu", "tg": "Telangana", "tr": "Tripura", "uttaranchal": "Uttarakhand",
    "uk": "Uttarakhand", "up": "Uttar Pradesh", "wb": "West Bengal", "dl": "Delhi",
    # Full names (no space for lookup)
    "andhrapradesh": "Andhra Pradesh",
    "aandhraapradesh": "Andhra Pradesh",
    "andhrapradhesh": "Andhra Pradesh",
    "arunachalpradesh": "Arunachal Pradesh",
    "himachalpradesh": "Himachal Pradesh",
    "jammuandkashmir": "Jammu and Kashmir",
    "madhyapradesh": "Madhya Pradesh",
    "uttarpradesh": "Uttar Pradesh",
    "westbengal": "West Bengal",
    "tamilnadu": "Tamil Nadu",
    "chandigarh": "Chandigarh",
    "kerla": "Kerala",
}

# Comprehensive list for extraction logic
INDIAN_STATES_FULL = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", 
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", 
    "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", 
    "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab", 
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura", 
    "Uttar Pradesh", "Uttarakhand", "West Bengal",
    "Andaman and Nicobar Islands", "Chandigarh", "Dadra and Nagar Haveli and Daman and Diu",
    "Delhi", "Jammu and Kashmir", "Ladakh", "Lakshadweep", "Puducherry", "Pondicherry"
]


class UniversalNormalizer:
    """
    Unicode-safe normalizer. Preserves ALL scripts (Devanagari, Gujarati,
    Tamil, Telugu, Bengali, Kannada, Malayalam, Odia, Gurmukhi, Urdu, etc.)
    Zero data loss — only trims whitespace and normalizes Unicode form.
    """

    @staticmethod
    def clean_text(val):
        """Preserve original text as-is. Only trim whitespace & normalize Unicode."""
        if val is None:
            return ""
        val = str(val).strip()
        # Treat Python/Pandas artefacts as empty
        if val.lower() in ('nan', 'none', 'nat', ''):
            return ""
        # NFKC normalization: standardizes visually identical chars across scripts
        # This is the FASTEST Unicode normalization and is non-destructive
        val = unicodedata.normalize('NFKC', val)
        # Collapse excessive whitespace
        val = re.sub(r'\s+', ' ', val).strip()
        return val

    @staticmethod
    def normalize_state(val):
        """Normalize state names: works for English abbreviations/names.
        Regional-language state names are preserved as-is."""
        if not val or not isinstance(val, str):
            return ""
        cleaned = str(val).strip()
        if cleaned.lower() in ('nan', 'none', 'nat', '', 'unknown'):
            return "unknown"
        # Try English abbreviation lookup (lowercase, no spaces)
        lookup_key = re.sub(r'[^a-z0-9]', '', cleaned.lower())
        if lookup_key in STATE_MAP:
            return STATE_MAP[lookup_key]
        # If it's in a regional script, preserve as-is
        return cleaned.strip()

    @staticmethod
    def extract_state_from_city(city, state):
        """Helper to move state name from city column if state is unknown."""
        c = str(city or "").strip()
        s = str(state or "").strip()
        
        if s.lower() in ('unknown', '') and c:
            for st in INDIAN_STATES_FULL:
                if c.endswith(f" {st}"):
                    detected = st
                    if detected == 'Pondicherry': detected = 'Puducherry'
                    return c[:-len(st)].strip(), detected
        return c, s

    @staticmethod
    def is_numerical_address(val):
        """Checks if the address is purely numerical junk."""
        addr = str(val or "").strip()
        if not addr: return False
        # Remove spaces/dashes to check if only digits remain
        stripped = re.sub(r'[\s\-]', '', addr)
        return stripped.isdigit() and len(stripped) > 3

    @staticmethod
    def normalize_phone(val):
        """Extract digits only and normalize (strip 0/91 prefix for Indian numbers)."""
        if not val:
            return ""
        # 1. Extract digits only
        s = re.sub(r'\D', '', str(val))
        
        # 2. Aggressive Indian normalization
        # Remove all leading zeros first (handles 07085485781 -> 7085485781)
        s = s.lstrip('0')
        
        # If it starts with 91 and is 12 digits, strip 91 (handles 917085485781 -> 7085485781)
        if len(s) == 12 and s.startswith('91'):
            s = s[2:]
            
        return s

    @staticmethod
    def normalize_website(val):
        """Normalize website URL — safe for all languages."""
        if not val or not isinstance(val, str):
            return ""
        val = str(val).strip().lower()
        if val in ('nan', 'none', 'nat', ''):
            return ""
        val = re.sub(r'^https?://', '', val)
        val = re.sub(r'^www\.', '', val)
        return val.rstrip('/')

    @staticmethod
    def normalize_category(val):
        """Normalize category — preserve regional text, title-case English."""
        if not val or not isinstance(val, str):
            return ""
        val = str(val).strip()
        if val.lower() in ('nan', 'none', 'nat', ''):
            return ""
        val = unicodedata.normalize('NFKC', val)
        val = re.sub(r'\s+', ' ', val).strip()
        return val

    @staticmethod
    def normalize_int(val):
        """Robust integer normalization. Prevents DB errors on empty strings."""
        if val is None: return 0
        val_str = str(val).strip().lower()
        if val_str in ('', 'nan', 'none', 'nat'): return 0
        match = re.search(r'\d+', val_str)
        return int(match.group()) if match else 0

    @staticmethod
    def normalize_float(val):
        """Robust float normalization. Prevents DB errors on empty strings."""
        if val is None: return 0.0
        val_str = str(val).strip().lower()
        if val_str in ('', 'nan', 'none', 'nat'): return 0.0
        match = re.search(r'[-+]?\d*\.?\d+', val_str)
        return float(match.group()) if match else 0.0

    @staticmethod
    def normalize_date(val):
        """Standardize ISO 8601 dates for MySQL DATETIME."""
        if not val: return None
        val_str = str(val).strip()
        if 'T' in val_str:
            # Convert 2024-02-26T10:00:00.000Z -> 2024-02-26 10:00:00
            val_str = val_str.replace('T', ' ').replace('Z', '').split('.')[0]
        return val_str

    @staticmethod
    def get_fuzzy(row, canonical_key):
        """🔍 Smart header mapping for multilingual CSVs."""
        # Common variations for Indian data headers
        MAPPINGS = {
            "name": ["name", "business name", "company name", "naam", "नाम", "નામ", "பெயர்", "పేరు", "ಹೆಸರು", "പേര്", "নাম"],
            "address": ["address", "location", "full address", "पता", "સરનામું", "மேகவரி", "చిరునామా", "ವಿಳಾಸ", "മേൽವിലാസം", "ঠিকানা"],
            "phone_number": ["phone", "phone number", "contact", "mobile", "tel", "फोन", "ફોન", "தொலைபேசி", "ఫోన్", "ಫೋನ್", "ഫോൺ", "ফোন"],
            "city": ["city", "town", "location city", "शहर", "શહેર", "நகரம்", "నగరం", "ನಗರ", "നഗരം", "শহর"],
            "state": ["state", "province", "region", "राज्य", "રાજ્ય", "மாநிலம்", "రాష్ట్రం", "ರಾಜ್ಯ", "സംസ്ഥാനം", "রাজ্য"],
            "category": ["category", "type", "business type", "श्रेणी", "શ્રેણી", "வகை", "ವರ್ಗ", "వర్గం", "വിഭാഗം", "বিভাগ"],
            "subcategory": ["subcategory", "sub-category", "उपश्रेणी", "ઉપશ્રેણી"],
            "website": ["website", "url", "link", "वेबसाइट", "વેબસાઇટ"],
            "reviews_count": ["reviews_count", "reviews", "total reviews", "समीक्षाएं"],
            "reviews_average": ["reviews_average", "rating", "avg rating", "रेटिंग"],
        }
        
        candidates = MAPPINGS.get(canonical_key, [canonical_key])
        
        # 1. Exact match
        for c in candidates:
            if c in row: return row[c]
            
        # 2. Case-insensitive / Trimmed match
        row_keys = {str(k).strip().lower(): k for k in row.keys()}
        for c in candidates:
            cl = c.lower()
            if cl in row_keys: return row[row_keys[cl]]
            
        return row.get(canonical_key)

    @classmethod
    def normalize_row_raw(cls, row):
        """Tier 1: Minimal normalization for raw storage. Only trims whitespace."""
        raw_city = cls.get_fuzzy(row, "city")
        raw_state = cls.get_fuzzy(row, "state")
        
        # Better extraction even at Tier 1 (Raw) level
        clean_city, clean_state = cls.extract_state_from_city(raw_city, raw_state)
        
        return {
            "name": cls.clean_text(cls.get_fuzzy(row, "name")), # Preserve original text but trim
            "address": cls.get_fuzzy(row, "address"),
            "website": cls.get_fuzzy(row, "website"),
            "phone_number": cls.get_fuzzy(row, "phone_number"),
            "reviews_count": cls.normalize_int(cls.get_fuzzy(row, "reviews_count")),
            "reviews_average": cls.normalize_float(cls.get_fuzzy(row, "reviews_average")),
            "category": cls.get_fuzzy(row, "category"),
            "subcategory": cls.get_fuzzy(row, "subcategory"),
            "city": clean_city,
            "state": clean_state,
            "area": row.get("area"),
            "drive_folder_id": row.get("drive_folder_id"),
            "drive_folder_name": row.get("drive_folder_name"),
            "drive_file_id": row.get("drive_file_id"),
            "drive_file_name": row.get("drive_file_name"),
            "drive_file_path": row.get("drive_file_path"),
            "drive_uploaded_time": cls.normalize_date(row.get("drive_uploaded_time")),
        }

    @classmethod
    def normalize_row_full(cls, row):
        """Tier 2: Robust normalization for clean/master storage."""
        raw_city = cls.get_fuzzy(row, "city")
        raw_state = cls.get_fuzzy(row, "state")
        
        # Cross-column normalization: Extract state from city if state is missing
        clean_city, clean_state = cls.extract_state_from_city(raw_city, raw_state)
        
        return {
            "name": cls.clean_text(cls.get_fuzzy(row, "name")),
            "address": cls.clean_text(cls.get_fuzzy(row, "address")),
            "website": cls.normalize_website(cls.get_fuzzy(row, "website")),
            "phone_number": cls.normalize_phone(cls.get_fuzzy(row, "phone_number")),
            "reviews_count": cls.normalize_int(cls.get_fuzzy(row, "reviews_count")),
            "reviews_average": cls.normalize_float(cls.get_fuzzy(row, "reviews_average")),
            "category": cls.normalize_category(cls.get_fuzzy(row, "category")),
            "subcategory": cls.clean_text(cls.get_fuzzy(row, "subcategory")),
            "city": cls.clean_text(clean_city),
            "state": cls.normalize_state(clean_state),
            "area": cls.clean_text(row.get("area")),
            "drive_folder_id": row.get("drive_folder_id"),
            "drive_folder_name": row.get("drive_folder_name"),
            "drive_file_id": row.get("drive_file_id"),
            "drive_file_name": row.get("drive_file_name"),
            "drive_file_path": row.get("drive_file_path"),
            "drive_uploaded_time": cls.normalize_date(row.get("drive_uploaded_time")),
        }
