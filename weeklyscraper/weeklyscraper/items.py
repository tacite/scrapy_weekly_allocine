# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field

class WeeklyscraperParsingItem(scrapy.Item):
    titre = Field()
    titre_original_reprise = Field()
    infos = Field()
    infos_technique = Field()
    realisateur = Field()
    only_realisateur = Field()
    nationalite = Field()
    description = Field()
    ratings = Field()
    duration = Field()
    public = Field()
    acteurs = Field()
    type = Field()
    
class WeeklyscraperItem(scrapy.Item):
    titre = Field()
    titre_original = Field()
    genre = Field()
    duration = Field()
    date_sortie = Field()
    annee_production = Field()
    date_reprise = Field()
    nationalite = Field()
    realisateur = Field()
    langues = Field()
    description = Field()
    notes_presse = Field()
    notes_spectateur = Field()
    public = Field()
    acteurs = Field()
    type = Field()