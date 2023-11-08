
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
