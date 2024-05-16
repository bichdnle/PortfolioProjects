-- SELECT *
-- FROM covid-19-423210.COVID_19.CovidDeaths
-- ORDER BY 3, 4;

-- SELECT *
-- FROM covid-19-423210.COVID_19.CovidVaccinations
-- ORDER BY 3, 4

-- Data used in the project
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covid-19-423210.COVID_19.CovidDeaths
ORDER BY 1, 2;

-- Total cases vs total deaths
-- Show the likelihood of dying if you contract COVID in your country
SELECT location, date, total_cases, total_deaths,
      ROUND((total_deaths/total_cases), 4) * 100 AS death_perc
FROM covid-19-423210.COVID_19.CovidDeaths
WHERE location LIKE '%States%'
ORDER BY 1, 2;

-- Total cases vs population
-- Percentage of population got COVID
SELECT location, date, total_cases, population,
      total_cases/population * 100 AS death_perc
FROM covid-19-423210.COVID_19.CovidDeaths
-- WHERE location LIKE '%States%'
ORDER BY 1, 2;

-- Countries with the highest infection rate compared to population
SELECT location, population, 
      MAX(total_cases) AS highest_case,
      MAX((total_cases/population) * 100) AS population_infected_perc
FROM covid-19-423210.COVID_19.CovidDeaths
-- WHERE location LIKE '%States%'
GROUP BY 1, 2
ORDER BY population_infected_perc DESC;

-- Highest death count per population
SELECT location, 
      MAX(total_deaths) AS highest_count
FROM covid-19-423210.COVID_19.CovidDeaths
-- WHERE location LIKE '%States%'
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY highest_count DESC;

-- Break things down by continent
SELECT continent, 
      MAX(total_deaths) AS highest_count
FROM covid-19-423210.COVID_19.CovidDeaths
-- WHERE location LIKE '%States%'
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY highest_count DESC;

-- Continent with the highest death count per population
SELECT continent, 
      MAX(total_deaths) AS highest_count 
FROM covid-19-423210.COVID_19.CovidDeaths
-- WHERE location LIKE '%States%'
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY highest_count DESC;

-- Global numbers
SELECT SUM(new_cases) AS total_cases,
      SUM(new_deaths) AS total_deaths,
      SUM(new_deaths)/SUM(new_cases) * 100 AS death_perc
FROM covid-19-423210.COVID_19.CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1, 2;

-- Total population vs vaccination
WITH pop_vs_vac AS (
      SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations,
            SUM(v.new_vaccinations) OVER(PARTITION BY d.location ORDER BY d.location, d.date) AS people_vac
      FROM covid-19-423210.COVID_19.CovidDeaths AS d
      JOIN covid-19-423210.COVID_19.CovidVaccinations v
        ON d.location = v.location AND d.date = v.date
      WHERE d.continent IS NOT NULL
)
SELECT *, (people_vac/population) * 100
FROM pop_vs_vac