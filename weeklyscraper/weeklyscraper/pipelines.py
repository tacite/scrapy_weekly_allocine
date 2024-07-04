# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from weeklyscraper.items import WeeklyscraperItem
from sqlalchemy import Column, Integer, Text, Numeric, create_engine, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base



class WeeklyscraperPipeline:
    def process_item(self, item, spider):
        parsingAdapter = ItemAdapter(item)
        weeklyItem = WeeklyscraperItem()
        
        weeklyAdapter = ItemAdapter(weeklyItem)
        weeklyAdapter['type'] = "clean"

        weeklyItem = self.clean_titre(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_titre_original_reprise(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_genre(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_duration(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_annee(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_annee_production(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_nationalite(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_realisateur(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_langues(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_description(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_ratings(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_public(weeklyItem, parsingAdapter, weeklyAdapter)
        weeklyItem = self.clean_acteurs(weeklyItem, parsingAdapter, weeklyAdapter)
        return weeklyItem
    
    def clean_titre(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        titre = parsingAdapter.get('titre')
        weeklyAdapter['titre'] = titre
        
        return weeklyItem
    
    def clean_titre_original_reprise(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        titre = parsingAdapter.get('titre_original_reprise')
        weeklyAdapter['titre_original'] = weeklyAdapter.get('titre')
        weeklyAdapter['date_reprise'] = "N/A"
        if titre:
            for index, value in enumerate(titre):
                if value == "Date de reprise":
                    weeklyAdapter['date_reprise'] = titre[index + 1].replace('\n', '')
                if value == "Titre original ":
                    weeklyAdapter['titre_original'] = titre[index + 1]
        return weeklyItem
    
    def clean_genre(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        infos = parsingAdapter.get('infos')
        final_pipe_index = max(index for index, item in enumerate(infos) if item == '|')
        weeklyAdapter['genre'] = infos[final_pipe_index + 1:]
        
        return weeklyItem
        
    def clean_duration(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        infos = parsingAdapter.get('duration')
        if all(element == '\n' for element in infos):
            value = 'N/A'
        else:
            value = next(value for value in infos if value != '\n').replace('\n', '')
        weeklyAdapter['duration'] = value
        
        return weeklyItem
    
    def clean_annee(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        infos = parsingAdapter.get('infos')
        annee_sortie = infos[0].replace('\n', '')
        weeklyAdapter['date_sortie'] = annee_sortie
        
        return weeklyItem
    
    def clean_annee_production(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        infos_technique = parsingAdapter.get('infos_technique')
        annee_production = infos_technique[infos_technique.index("Année de production") + 1]
        weeklyAdapter['annee_production'] = annee_production
        
        return weeklyItem
        
    def clean_nationalite(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        nationalite = parsingAdapter.get('nationalite')
        weeklyAdapter['nationalite'] = nationalite
        
        return weeklyItem
    
    def clean_realisateur(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        realisateur = parsingAdapter.get('realisateur')
        tmp = parsingAdapter.get('only_realisateur')
        if realisateur:
            real = realisateur[1:realisateur.index('Par')]
        elif tmp:
            real = tmp[1:]
        else:
            real = 'N/A'
        weeklyAdapter['realisateur'] = real
        return weeklyItem
    
    def clean_langues(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        infos_technique = parsingAdapter.get('infos_technique')
        langues = list(string.replace('\n', '') for string in infos_technique[infos_technique.index("Langues") + 1: infos_technique.index("Format production")])
        langues = list(elem[1:] if elem[0] == ' ' else elem for elem in langues[0].split(','))
        
        if langues == ['-']:
            weeklyAdapter['langues'] = []
        else:
            weeklyAdapter['langues'] = langues
        
        return weeklyItem
    
    def clean_description(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        description = parsingAdapter.get('description')
        weeklyAdapter['description'] = description
        
        return weeklyItem
    
    def clean_ratings(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        ratings = parsingAdapter.get('ratings')
        
        def filter_list(string: str) -> bool:
            if string == '\n' or 'critique' in string  or 'note' in string:
                return False
            return True
        
        cleaned = list(filter(filter_list, ratings))
        weeklyAdapter['notes_presse'] = None
        weeklyAdapter['notes_spectateur'] = None
        for index, value in enumerate(cleaned):
            if value == " Presse ":
                weeklyAdapter['notes_presse'] = cleaned[index + 1].replace(',', '.')
            if value == " Spectateurs ":
                weeklyAdapter['notes_spectateur'] = cleaned[index + 1].replace(',', '.')
            
        return weeklyItem
    
    def clean_public(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        public = parsingAdapter.get('public')
        
        weeklyAdapter['public'] = public if public is not None else 'N/A'
        
        return weeklyItem
    
    def clean_acteurs(self,weeklyItem: WeeklyscraperItem, parsingAdapter: ItemAdapter, weeklyAdapter: ItemAdapter):
        acteurs = parsingAdapter.get('acteurs')
        acteurs = list(acteur for acteur in acteurs if '\n' not in acteur)
        if len(acteurs) > 8:
            weeklyAdapter['acteurs'] = acteurs[:7]
        else:
            weeklyAdapter['acteurs'] = acteurs
        
        return weeklyItem
    
class DatabasePipeline:
    Base = declarative_base()
    
    class Film(Base):
        __tablename__ = "film"
        id = Column(Integer, primary_key=True, autoincrement=True)
        titre = Column(Text)
        titre_original = Column(Text)
        duration = Column(Text)
        date_sortie = Column(Text)
        annee_production = Column(Text)
        date_reprise = Column(Text)
        description = Column(Text)
        notes_presse = Column(Numeric)
        notes_spectateur = Column(Numeric)
        public = Column(Text)

        genres = relationship('Genre', back_populates='film', cascade='all, delete-orphan')
        nationalites = relationship('Nationalite', back_populates='film', cascade='all, delete-orphan')
        realisateurs = relationship('Realisateur', back_populates='film', cascade='all, delete-orphan')
        acteurs = relationship('Acteur', back_populates='film', cascade='all, delete-orphan')
        langues = relationship('Langue', back_populates='film', cascade='all, delete-orphan')
    
    class Langue(Base):
        __tablename__ = 'langue'
    
        id = Column(Integer, primary_key=True)
        film_id = Column(Integer, ForeignKey('film.id'))
        langue = Column(Text, nullable=False)

        film = relationship('Film', back_populates='langues')
    
    
    class Genre(Base):
        __tablename__ = 'genre'

        id = Column(Integer, primary_key=True, autoincrement=True)
        film_id = Column(Integer, ForeignKey('film.id'))
        genre = Column(Text, nullable=False)

        film = relationship('Film', back_populates='genres')
        
    class Nationalite(Base):
        __tablename__ = 'nationalite'

        id = Column(Integer, primary_key=True, autoincrement=True)
        film_id = Column(Integer, ForeignKey('film.id'))
        nationalite = Column(Text, nullable=False)

        film = relationship('Film', back_populates='nationalites')

    class Realisateur(Base):
        __tablename__ = 'realisateur'

        id = Column(Integer, primary_key=True, autoincrement=True)
        film_id = Column(Integer, ForeignKey('film.id'))
        realisateur = Column(Text, nullable=False)

        film = relationship('Film', back_populates='realisateurs')
    
    class Acteur(Base):
        __tablename__ = 'acteur'

        id = Column(Integer, primary_key=True, autoincrement=True)
        film_id = Column(Integer, ForeignKey('film.id'))
        acteur = Column(Text, nullable=False)

        film = relationship('Film', back_populates='acteurs')
    
    def open_spider(self, spider):
        
        username = "postgres"
        password = "3}~pg09B"
        hostname = "davidtacite.postgres.database.azure.com"
        port = 5432
        database = "postgres"
                
        connection_string = f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}'
        engine = create_engine(connection_string)
        self.Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        self.session = Session()
        
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        notes_presse = float(adapter.get('notes_presse')) if adapter.get('notes_presse') is not None else None
        notes_spectateur = float(adapter.get('notes_spectateur')) if adapter.get('notes_spectateur') is not None else None
        langues = adapter.get('langues')
        nationalite = adapter.get('nationalite')
        realisateur = adapter.get('realisateur')
        acteur = adapter.get('acteurs')
        genre = adapter.get('genre')
        film = self.Film(titre=adapter.get('titre'), titre_original=adapter.get('titre_original'),
                        duration=adapter.get('duration'), date_sortie=adapter.get('date_sortie'),
                        annee_production=adapter.get('annee_production'), date_reprise=adapter.get("date_reprise"),
                        description=adapter.get('description'), notes_presse=notes_presse,
                        notes_spectateur=notes_spectateur, public=adapter.get('public'))
        self.session.add(film)
        self.session.flush()
        for value in langues:
            langue = self.Langue(film_id=film.id, langue=value)
            self.session.add(langue)
        for value in nationalite:
            new = self.Nationalite(film_id=film.id, nationalite=value)       
            self.session.add(new) 
        for value in realisateur:
            new = self.Realisateur(film_id=film.id, realisateur=value)       
            self.session.add(new) 
        for value in acteur:
            new = self.Acteur(film_id=film.id, acteur=value)       
            self.session.add(new) 
        for value in genre:
            new = self.Genre(film_id=film.id, genre=value)       
            self.session.add(new) 
            
        self.session.commit()
        return item
        
        
    def close_spider(self, spider):
        self.session.close()