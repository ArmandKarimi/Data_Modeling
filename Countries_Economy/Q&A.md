### 1. 🔍 Countries and Languages — Basic Join

```sql
SELECT country_name, l.name
FROM countries c
JOIN languages l ON c.code = l.code
LIMIT 10;
```

---

### 2. 📊 Number of Languages Spoken per Country

```sql
SELECT country_name, COUNT(*) AS num_lang
FROM countries c
JOIN languages l ON c.code = l.code
GROUP BY country_name
ORDER BY num_lang DESC
LIMIT 10;
```

---

### 3. 📈 Fertility & Unemployment Rate by Country/Year

```sql
SELECT country_name,
       e.year,
       p.fertility_rate,
       e.unemployment_rate
FROM countries c
JOIN population p ON p.country_code = c.code
JOIN economies e ON e.code = c.code
WHERE p.year = e.year
ORDER BY country_name, year DESC
LIMIT 10;
```

---

### 4. 🏙️ Cities and Country Info (INNER JOIN vs LEFT JOIN)

```sql
-- INNER JOIN
SELECT c.name,
       c2.country_name,
       c2.region,
       city_proper_pop,
       metroarea_pop
FROM cities c
INNER JOIN countries c2 ON c.country_code = c2.code
LIMIT 10;

-- LEFT JOIN
SELECT c.name,
       c2.country_name,
       c2.region,
       city_proper_pop,
       metroarea_pop
FROM cities c
LEFT JOIN countries c2 ON c.country_code = c2.code
LIMIT 10;
```

---

### 5. 🌍 Average GDP Per Capita by Region (2010)

```sql
SELECT AVG(e.gdp_percapita), c.region
FROM economies e
JOIN countries c USING (code)
GROUP BY c.region, e.year
HAVING e.year = 2010;
```

---

### 6. 🧍‍♂️👬 Population Growth from 2010 to 2015

```sql
SELECT c.country_name,
       p1.size AS size2010,
       p2.size AS size2015
FROM population p1
JOIN population p2 USING (country_code)
JOIN countries c ON p1.country_code = c.code
WHERE p1.year = 2010
  AND p2.year = 2015;
```

---

### 7. ➕ UNION — Combine Results from Two Queries

```sql
SELECT * 
FROM languages
WHERE code = 'AFG'

UNION

SELECT * 
FROM languages
WHERE code = 'USA'

ORDER BY code;
```

---

### 8. 🤝 Semi Join — Languages Spoken in the Middle East

```sql
SELECT l.name
FROM languages l
WHERE l.code IN (
  SELECT code
  FROM countries
  WHERE region = 'Middle East'
);
```

---

### 9. 📊 Subquery in WHERE — Above-Average Fertility

```sql
SELECT *
FROM population p
WHERE year = 2015
  AND life_expectancy > 1.15 * (
    SELECT AVG(life_expectancy)
    FROM population p2
    WHERE year = 2010
);
```

---

### 10. 🏙️ Capital Cities Ordered by Urban Population Size

```sql
SELECT name, country_code, urbanarea_pop
FROM cities
WHERE name IN (
  SELECT capital
  FROM countries
);
```

---

### 11. 📈 Top Countries by City Count (Using Subquery)

```sql
SELECT country_code, COUNT(name) AS num_cities
FROM cities
GROUP BY country_code
ORDER BY num_cities DESC
LIMIT 9;
```

---

### 12. 🗣️ Number of Languages Per Country (JOIN)

```sql
SELECT c.local_name, COUNT(l.name)
FROM languages l
JOIN countries c ON l.code = c.code
GROUP BY c.local_name
LIMIT 10;
```

---

### 13. 🧠 Subquery in FROM — Languages per Country

```sql
SELECT local_name, num_langs
FROM (
  SELECT c.local_name, COUNT(l.name) AS num_langs
  FROM countries c
  JOIN languages l ON c.code = l.code
  GROUP BY c.local_name
) AS sub;
```
