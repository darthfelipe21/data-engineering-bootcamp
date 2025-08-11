SELECT 
    MAX(event_time),
    MIN(event_time)
FROM events;

-- SELECT CURRENT_DATE + (7 || ' days')::INTERVAL; al dia actual le agrega 7 y luego cocatena con 'days' para poder indicar el intervalos de dias que debe de ir en string
-- (number_of_days || ' days')::INTERVAL == CAST(number_of_days || ' days' AS INTERVAL)

CREATE TABLE users_cumulated(
    user_id NUMERIC,
    -- List of dates in the past where the user was active
    dates_active DATE[],
    -- The current date for the user
    date DATE,
    PRIMARY KEY (user_id, date)
    );


-- Ingresar fechas de manera incremental desde el 2023-01-01 al 2023-01-31 uno a uno
INSERT INTO users_cumulated
WITH yesterday AS(
    SELECT *
    FROM users_cumulated 
    WHERE date = DATE('2023-01-30') -- Incrementar uno

), today AS (
    SELECT
        user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31') AND user_id IS NOT NULL -- Incrementar uno
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
         WHEN t.date_active IS NULL THEN y.dates_active 
         ELSE ARRAY[t.date_active] || y.dates_active -- || sirve para concatenar cadenas de texto, en este caso agregar el valor de y.dates_active al array
         END AS dates_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;


-- Transformar a day list
-- Genera CTE 'users' con los datos de actividad acumulada del último día del mes
WITH users AS (
    SELECT * 
    FROM users_cumulated 
    WHERE date = DATE('2023-01-31')
-- Crea una serie de fechas día por día entre el 1 y el 31 de enero
), series AS (
    SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
-- Calcula para cada usuario y fecha si estuvo activo, y genera un valor binario codificado
), placeholder_ints AS (
SELECT
    -- Si el usuario estuvo activo en esa fecha, genera el valor POW(2, posición en el mes); si no, devuelve 0
   CASE WHEN
        dates_active @> ARRAY [DATE(series_date)] -- Si la fecha dates_active esta dentro del arreglo
    THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
    ELSE 0
    END AS placeholder_int_value,
    *
FROM users
CROSS JOIN series
)
-- Agrupa por usuario y calcula el bitmask de actividad mensual
SELECT user_id,
       -- Suma todos los valores binarios para ese usuario
       SUM(placeholder_int_value),
       -- Convierte el número total en una representación de bits tipo BIT(32)
       CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
       -- Verificar si el usuario estuvo activo dentro del mes
       BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_monthly_active,
       -- Verificar si el usuario estuvo activo dentro de la semana
       BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_weekly_active, -- bit a bit (bitwise) entre dos valores tipo BIT(32)
       -- Verificar si el usuario estuvo activo diariamente
       BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_daily_active
FROM placeholder_ints
GROUP BY user_id;
-- Resultado: número donde cada bit encendido representa un día activo




-- CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
-- Esto genera un valor tipo bitmask para esa fecha.
-- POW(2, ...) calcula una potencia de 2, por ejemplo 2^1 = 2, 2^2 = 4, etc.
-- La expresión 32 - (date - series_date) define la posición del bit, desde el MSB (bit más alto) hacia el LSB.
-- Si date = '2023-01-31' y series_date = '2023-01-01', entonces date - series_date = 30
-- Lo que da: 32 - 30 = 2, por lo tanto POW(2, 2) = 4


-- CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
-- Sumando todos los valores individuales generados por POW(2, ...)
-- Esto da un número donde cada bit encendido representa un día activo
-- Luego lo conviertes a un tipo BIT(32)
-- Esto produce un campo tipo 010010100000... con hasta 32 bits
-- Cada posición representa un día entre el 1 y el 31 de enero

-- Al hacer 32 - (date - series_date), estamos dejando el bit más alto (2^31) como representación del primer día del mes y bajando hacia el último.
