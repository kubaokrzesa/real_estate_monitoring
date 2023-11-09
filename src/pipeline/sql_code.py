
survey_tab_drop = "DROP TABLE IF EXISTS surveys"

survey_tab_creation = """
CREATE TABLE IF NOT EXISTS surveys
(
    survey_id TEXT UNIQUE,
    survey_date DATE,
    survey_type TEXT,
    location TEXT,
    n_pages INT
)

"""

survey_table_insert = """INSERT INTO surveys (survey_id, survey_date, survey_type, location, n_pages) 
                 VALUES (?, ?, ?, ?, ?);"""


survey_links_tab_creation = """
CREATE TABLE IF NOT EXISTS survey_links
(
    survey_id TEXT,
    link TEXT,
    UNIQUE(survey_id, link)
)

"""

scraped_offers_tab_creation = """
CREATE TABLE IF NOT EXISTS scraped_offers
(
    survey_id TEXT,
    link TEXT,
    title TEXT,
    price TEXT,
    adress TEXT,
    sq_m_price TEXT,
    area TEXT,
    ownership_type TEXT,
    n_rooms TEXT,
    state TEXT,
    floor TEXT,
    rent TEXT,
    remote TEXT,
    balcony TEXT,
    heating TEXT,
    parking TEXT,
    market TEXT,
    offerent_type TEXT,
    available_from TEXT,
    year_built TEXT,
    building_type TEXT,
    windows TEXT,
    elevator TEXT,
    media TEXT,
    safety TEXT,
    equipment TEXT,
    additional_info TEXT,
    building_material TEXT,
    description TEXT,
    UNIQUE(survey_id, link)
)
"""

numeric_feature_tab_creation = """
CREATE TABLE IF NOT EXISTS numeric_features
(
    survey_id TEXT,
    link TEXT,
    price REAL,
    sq_m_price REAL,
    area REAL,
    n_rooms INTEGER,
    rent REAL,
    floor INTEGER,
    max_floor INTEGER,
    age_num INTEGER,
    days_till_available REAL,
    UNIQUE(survey_id, link)
)
"""

categorical_feature_tab_creation = """
CREATE TABLE IF NOT EXISTS categorical_features
(
    survey_id TEXT,
    link TEXT,
    ownership_type_MISSING REAL,
    ownership_type_pelna_wlasnosc REAL,
    ownership_type_spoldzielcze_wl_prawo_do_lokalu REAL,
    ownership_type_udzial REAL,
    ownership_type_uzytkowanie_wieczyste_dzierzawa REAL,
    state_MISSING REAL,
    state_do_remontu REAL,
    state_do_wykonczenia REAL,
    state_do_zamieszkania REAL,
    remote_MISSING REAL,
    remote_tak REAL,
    heating_MISSING REAL,
    heating_elektryczne REAL,
    heating_gazowe REAL,
    heating_inne REAL,
    heating_kotlownia REAL,
    heating_miejskie REAL,
    heating_piece_kaflowe REAL,
    parking_MISSING REAL,
    parking_garazmiejsce_parkingowe REAL,
    market_MISSING REAL,
    market_pierwotny REAL,
    market_wtorny REAL,
    offerent_type_MISSING REAL,
    offerent_type_biuro_nieruchomosci REAL,
    offerent_type_deweloper REAL,
    offerent_type_prywatny REAL,
    building_type_MISSING REAL,
    building_type_apartamentowiec REAL,
    building_type_blok REAL,
    building_type_brak_informacji REAL,
    building_type_dom_wolnostojacy REAL,
    building_type_dworekpalac REAL,
    building_type_kamienica REAL,
    building_type_loft REAL,
    building_type_plomba REAL,
    building_type_szeregowiec REAL,
    windows_MISSING REAL,
    windows_aluminiowe REAL,
    windows_brak_informacji REAL,
    windows_drewniane REAL,
    windows_plastikowe REAL,
    elevator_MISSING REAL,
    elevator_nie REAL,
    elevator_tak REAL,
    building_material_MISSING REAL,
    building_material_beton REAL,
    building_material_beton_komorkowy REAL,
    building_material_brak_informacji REAL,
    building_material_cegla REAL,
    building_material_drewno REAL,
    building_material_inne REAL,
    building_material_keramzyt REAL,
    building_material_pustak REAL,
    building_material_silikat REAL,
    building_material_wielka_plyta REAL,
    building_material_zelbet REAL,
    UNIQUE(survey_id, link)
)
"""

label_feature_tab_creation = """
CREATE TABLE IF NOT EXISTS label_features
(
    survey_id TEXT,
    link TEXT,

    balcony_NO REAL,
    balcony_balkon REAL,
    balcony_ogrodek REAL,
    balcony_taras REAL,
    media_NO REAL,
    media_brak_informacji REAL,
    media_gaz REAL,
    media_internet REAL,
    media_kanalizacja REAL,
    media_prad REAL,
    media_telefon REAL,
    media_telewizja_kablowa REAL,
    media_woda REAL,
    safety_NO REAL,
    safety_brak_informacji REAL,
    safety_domofon_wideofon REAL,
    safety_drzwi_okna_antywlamaniowe REAL,
    safety_monitoring_ochrona REAL,
    safety_rolety_antywlamaniowe REAL,
    safety_system_alarmowy REAL,
    safety_teren_zamkniety REAL,
    equipment_NO REAL,
    equipment_brak_informacji REAL,
    equipment_kuchenka REAL,
    equipment_lodowka REAL,
    equipment_meble REAL,
    equipment_piekarnik REAL,
    equipment_pralka REAL,
    equipment_telewizor REAL,
    equipment_zmywarka REAL,
    additional_info_NO REAL,
    additional_info_brak_informacji REAL,
    additional_info_dwupoziomowe REAL,
    additional_info_oddzielna_kuchnia REAL,
    additional_info_piwnica REAL,
    additional_info_pom_uzytkowe REAL,
    UNIQUE(survey_id, link)
)
"""

geocoded_adr_tab_creation = """
CREATE TABLE IF NOT EXISTS geocoded_adr
(
    survey_id TEXT,
    link TEXT,
    address TEXT,
    latitude REAL,
    longitude REAL,
    UNIQUE(survey_id, link)
)
"""
