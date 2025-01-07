# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich preferencií vo výbere filmov na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrík.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v preferenciách divákov, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z Kaggle datasetu dostupného [tu](https://www.kaggle.com/datasets/grouplens/movielens-20m-dataset/data). Dataset obsahuje päť hlavných tabuliek:
- `genome_scores`
- `genome_tags`
- `movie`
- `rating`
- `tag`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/Sebi26S/MovieLens/blob/main/erd_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Diagram entity a vzťahov databázu MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol hviezdicový model (star schema), pre efektívnu analýzu, kde centrálny bod predstavuje faktová tabuľka fact_ratings, ktorá je prepojená s nasledujúcimi dimenziami:

- **`DIM_movie`**: Obsahuje podrobné informácie o filmoch (ID filmu, názov, žánre).
- **`DIM_tag`**: Obsahuje informácie o značkách (ID značky, názov značky).
- **`DIM_user`**: Obsahuje informácie o používateľoch (ID používateľa).
- **`DIM_time`**: Zahrňuje podrobné časové údaje (deň, mesiac, rok, deň v týždni, hodina, minúta, sekunda).

Štruktúra hviezdicového modelu zjednodušuje pochopenie a implementáciu modelu pre analýzu dát z MovieLens datasetu.

<p align="center">
  <img src="https://github.com/Sebi26S/MovieLens/blob/main/etl_schema.png" alt="Star Schema">
  <br>
  <em>Obrázok 2: Schéma hviezdy pre MovieLens

</em>
</p>

---

## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**

Počas tejto fázy boli dáta zo zdrojových CSV súborov úspešne importované do staging tabuliek v Snowflake. Tento proces tvorí základ pre ďalšie kroky spracovania a transformácie dát, pričom zabezpečuje ich dostupnosť a pripravenosť na analýzu.

```sql
CREATE DATABASE IF NOT EXISTS FOX_MovieLens_DB;
USE DATABASE FOX_MovieLens_DB;

CREATE SCHEMA IF NOT EXISTS FOX_MovieLens_DB.STAGING;
USE SCHEMA FOX_MovieLens_DB.STAGING;

CREATE OR REPLACE STAGE FOX_MovieLens_DB_STAGE
```

Do stage úložiska boli nahraté súbory obsahujúce údaje o filmoch, používateľoch, hodnoteniach a tagoch. Tieto súbory boli následne importované do staging tabuliek pomocou príkazu COPY INTO. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO movie_staging
FROM @FOX_MovieLens_DB_STAGE/movie.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';
```

Parameter ON_ERROR = 'CONTINUE' zabezpečil, že proces importovania dát pokračoval aj v prípade, ak sa vyskytli nekonzistentné záznamy, čím sa minimalizovali prerušenia počas ETL procesu.

---
### **3.2 Transform (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Fact_Table:
```sql
CREATE OR REPLACE TABLE Fact_Table AS
SELECT
  ROW_NUMBER() OVER (ORDER BY r.movieid, r.userid) AS factid,
  r.movieid               AS DIM_movie_movieid,
  r.userid                AS DIM_user_userid,
  gt.tagId                AS DIM_tag_tagid,
  r.rating                AS rating,
  gs.relevance            AS relevance,
  d.dim_timeID            AS DIM_time_timeID
FROM rating_staging r
LEFT JOIN genome_scores_staging gs
  ON r.movieid = gs.movieId
LEFT JOIN genome_tags_staging gt
  ON gs.tagId = gt.tagId
JOIN DIM_time d
  ON CAST(r.timestamp AS DATE) = d.date;

  SELECT 
    factid,
    DIM_movie_movieid,
    DIM_user_userid,
    DIM_tag_tagid,
    rating,
    relevance,
    DIM_time_timeID
FROM Fact_Table
LIMIT 10;
```
  DIM_time:
```sql

CREATE OR REPLACE TABLE DIM_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(timestamp AS DATE)) AS dim_timeID,
    CAST(timestamp AS DATE) AS date,
    DATE_PART('day', timestamp) AS day,
    DATE_PART('dow', timestamp) + 1 AS dayOfWeek,
    CASE DATE_PART('dow', timestamp) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS dayOfWeekAsString,
    DATE_PART('month', timestamp) AS month,
    DATE_PART('year', timestamp) AS year,
    DATE_PART('quarter', timestamp) AS quarter,
    DATE_PART('hour', timestamp) AS hour,
    DATE_PART('minute', timestamp) AS minute,
    DATE_PART('second', timestamp) AS second
FROM rating_staging;
```
  DIM_user:
```sql
CREATE OR REPLACE TABLE DIM_user AS
SELECT DISTINCT
  userid AS userid
FROM rating_staging;
```
  DIM_tag:
```sql
CREATE OR REPLACE TABLE DIM_tag AS
SELECT DISTINCT
  tagId AS tagid,
  tag   AS tag
FROM genome_tags_staging;
```
  DIM_movie:
```sql
CREATE OR REPLACE TABLE DIM_movie AS
SELECT DISTINCT
  movieId   AS movieid,
  title     AS title,
  genres    AS genres
FROM movie_staging;
```
---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS genome_scores_staging;
DROP TABLE IF EXISTS genome_tags_staging;
DROP TABLE IF EXISTS rating_staging;
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS users_staging;
```
---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, používateľov a ich hodnotení. Tieto vizualizácie odpovedajú na zásadné otázky a umožňujú hlbšie pochopiť preferencie divákov a ich správanie.

<p align="center">
  <img src="https://github.com/Sebi26S/MovieLens/blob/main/Vizualizations.png" alt="Vizualizations">
  <br>
  <em>Obrázok 3 Dashboard pre MovieLens datasetu</em>
</p>

---
### **Graf 1:Average Rating by Day of Week**
Táto vizualizácia zobrazuje priemerné hodnotenie podľa dní v týždni. Umožňuje identifikovať, v ktoré dni používatelia zvyknú pridávať hodnotenia s vyššími priemernými známkami.
```sql
SELECT 
    d.dayOfWeekAsString, 
    AVG(f.rating) AS avg_rating
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.dayOfWeekAsString, DATE_PART('dow', d.date)
ORDER BY DATE_PART('dow', d.date);
```
---
### **Graf 2:Top 10 Users by Number of Ratings**
Táto vizualizácia zobrazuje 10 najaktívnejších používateľov podľa počtu hodnotení. Umožňuje identifikovať používateľov, ktorí sú najviac angažovaní pri hodnotení filmov. 
```sql
SELECT 
    f.DIM_user_userid, 
    COUNT(f.factid) AS rating_count
FROM Fact_Table f
GROUP BY f.DIM_user_userid
ORDER BY rating_count DESC
LIMIT 10;
```
---
### **Graf 3: Hourly Rating Activity**
Tento graf znázorňuje aktivitu používateľov pri hodnotení filmov podľa hodín dňa. Z vizualizácie je možné identifikovať časové obdobia s najväčšou aktivitou.

```sql
SELECT 
    d.hour, 
    COUNT(f.factid) AS rating_count
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.hour
ORDER BY d.hour;
```
---
### **Graf 4: Total Ratings by Month**
Tento graf zobrazuje celkový počet hodnotení rozdelený podľa mesiacov v roku. Vizualizácia umožňuje identifikovať obdobia s najvyššou hodnotiacou aktivitou.

```sql
SELECT 
    d.month, 
    COUNT(f.factid) AS total_ratings
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.month
ORDER BY d.month;
```
---
### **Ratings Trend by Year**
Tento graf znázorňuje počet hodnotení podľa rokov, čo umožňuje sledovať dlhodobé trendy v aktivite používateľov.

```sql
SELECT 
    d.year, 
    COUNT(f.factid) AS total_ratings
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.year
ORDER BY d.year;
```
---
### **Graf 6: Most rated movies**
Tento graf zobrazuje 10 filmov s najväčším počtom hodnotení. Umožňuje identifikovať najpopulárnejšie tituly medzi používateľmi, čo poskytuje prehľad o tom, ktoré filmy si získali najväčšiu pozornosť.

```sql
SELECT 
    m.title AS movie_title,
    COUNT(f.factid) AS rating_count
FROM Fact_Table f
JOIN DIM_movie m ON f.DIM_movie_movieid = m.movieid
GROUP BY m.title
ORDER BY rating_count DESC
LIMIT 10;

```
---

**Autor:** Sebestyén Svajcer

