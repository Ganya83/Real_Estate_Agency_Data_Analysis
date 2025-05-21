-- 1.
-- Подсчитайте, за какой период представлены объявления о продаже недвижимости. Выберите диапазон значений — минимальную и максимальную даты.
select min(first_day_exposition),
       max(first_day_exposition)
from real_estate.advertisement;


--max       |min       |
------------+----------+
--2019-05-03|2014-11-27|



-- 2.
-- Изучите распределение объявлений по населённым пунктам в зависимости от их типа и подсчитайте для каждого типа количество населённых пунктов и количество объявлений. 
select type, count(id) AS ob
FROM real_estate.flats AS f
LEFT JOIN real_estate.type AS t ON f.type_id=t.type_id
GROUP BY type;

--type                                     |ob   |
-------------------------------------------+-----+
--село                                     |   32|
--садовое товарищество                     |    4|
--садоводческое некоммерческое товарищество|    1|
--деревня                                  |  945|
--посёлок городского типа                  |  363|
--городской посёлок                        |  187|
--посёлок                                  | 2092|
--коттеджный посёлок                       |    3|
--город                                    |20008|
--посёлок при железнодорожной станции      |   15|


SELECT 
t.type,
count(DISTINCT city_id) AS cities_count -- city_id населённый пунктов, считаем число населённых пунктов без повторов 
FROM real_estate.flats f 
JOIN real_estate."type" t ON t.type_id = f.type_id
GROUP BY t.type_id -- говорим группировать по типу населённого пункта 
ORDER BY cities_count DESC; 

--type                                     |cities_count|
-------------------------------------------+------------+
--посёлок                                  |         113|
--деревня                                  |         106|
--город                                    |          43|
--посёлок городского типа                  |          30|
--городской посёлок                        |          13|
--село                                     |           9|
--посёлок при железнодорожной станции      |           6|
--садовое товарищество                     |           4|
--коттеджный посёлок                       |           3|
--садоводческое некоммерческое товарищество|           1|



-- 3.
-- Подсчитайте основные статистики по полю со временем активности объявлений. 
SELECT min(days_exposition),
       max(days_exposition),
       AVG(days_exposition),
       percentile_disc(0.5) WITHIN GROUP(ORDER BY days_exposition)
FROM real_estate.advertisement;  

--min|max   |avg              |percentile_disc|
-----+------+-----------------+---------------+
--1.0|1580.0|180.7531998045921|           95.0|



-- 4.
-- Рассчитайте процент объявлений, которые сняли с публикации. Результат округлите до двух знаков после запятой и представьте в формате NN.NN       
SELECT count(id) AS total_id,
       count (days_exposition) AS sale_id,
       ROUND(count (days_exposition)::NUMERIC*100/count(id), 2) AS share_sale
FROM real_estate.advertisement;

--total_id|sale_id|share_sale|
----------+-------+----------+
--   23650|  20470|     86.55|



--5. 
-- Какой процент квартир продаётся в Санкт-Петербурге?
with count_t1 as (select count(a.id) as count_spb
                from real_estate.flats f 
                left join real_estate.advertisement a on f.id = a.id 
                left join real_estate.city c on f.city_id = c.city_id 
                where city = 'Санкт-Петербург'),
    count_t2 as (select count(id) as count_total
                from real_estate.advertisement) 
select  
      round(count_spb::numeric*100/count_total, 2) 
from count_t1,count_t2;

--round|
-------+
--66.47|



--6. 
--Подсчитайте основные статистические показатели для значений стоимости одного квадратного метра
SELECT 
       ROUND(min(last_price/total_area)::NUMERIC, 2),
       MAX(last_price/total_area),
       ROUND(AVG(last_price/total_area)::NUMERIC, 2),
       percentile_disc(0.5) WITHIN GROUP(ORDER BY last_price/total_area)
FROM real_estate.flats f
JOIN real_estate.advertisement a ON f.id=a.id;

--round |max      |round   |percentile_disc|
--------+---------+--------+---------------+
--111.84|1907500.0|99432.25|        95000.0|



--7.
--Проверьте корректность данных и подсчитайте статистические показатели — минимальное и максимальное значения, среднее значение, медиану и 99 перцентиль по следующим количественным данным:
-- общая площадь недвижимости, количество комнат и балконов, высота потолков, этаж. Выберите верные утверждения.
SELECT 
       min(total_area) AS min_ar,----статистика по общей площади недвижимости
       MAX(total_area) AS max_ar,
       ROUND(AVG(total_area)::NUMERIC, 2) AS avg_ar,
       percentile_disc(0.5) WITHIN GROUP(ORDER BY total_area) AS med_ar,
       percentile_disc(0.99) WITHIN GROUP(ORDER BY total_area) AS p99_ar,
       min(rooms) AS min_r,--статистика по количесву комнат
       MAX(rooms) AS max_r,
       AVG(rooms) AS avg_rs,
       percentile_disc(0.5) WITHIN GROUP(ORDER BY rooms) AS med_r,
       percentile_disc(0.99) WITHIN GROUP(ORDER BY rooms) AS p99_r,
       min(balcony) AS min_b,--статистика по количеству балконов
       MAX(balcony) AS max_b,
       AVG(balcony) AS avg_b,
       percentile_disc(0.5) WITHIN GROUP(ORDER BY balcony) AS med_b,
       percentile_disc(0.99) WITHIN GROUP(ORDER BY balcony) AS p99_b,
       min(ceiling_height) AS min_ch,--статистика по высоте потолка
       MAX(ceiling_height) AS max_ch,
       AVG(ceiling_height) AS avg_ch,
       percentile_disc(0.5) WITHIN GROUP(ORDER BY ceiling_height) AS med_ch,
       percentile_disc(0.99) WITHIN GROUP(ORDER BY ceiling_height) AS p99_ch,
       min(floor) AS min_f,--статистика по этажу
       AVG(floor) AS avg_f,
       percentile_disc(0.5) WITHIN GROUP(ORDER BY floor) AS med_f,
       percentile_disc(0.99) WITHIN GROUP(ORDER BY floor) AS p99_f
FROM real_estate.flats;



--Решаем ad hoc задачи:
--1.
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS
    (SELECT  
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
          PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS
    (SELECT id
    FROM real_estate.flats  
    WHERE total_area < (SELECT total_area_limit FROM limits)
          AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
          AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
          AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)),
category AS
    (SELECT *,
     CASE 
        WHEN city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
        ELSE 'ЛенОбл'
    END AS region,
     CASE 
        WHEN days_exposition BETWEEN 1 AND 30 THEN 'до месяца'
        WHEN days_exposition BETWEEN 31 AND 90 THEN 'до трех месяцев'
        WHEN days_exposition BETWEEN 91 AND 180 THEN 'до полугода'
        WHEN days_exposition > 180 THEN 'больше полугода'
        ELSE 'прочие'
     END AS activity_time,
        last_price/total_area AS metr_price
    FROM real_estate.advertisement AS a 
    JOIN real_estate.flats AS f ON a.id=f.id
    JOIN real_estate.city AS c ON c.city_id=f.city_id
    WHERE f.id IN (SELECT id FROM filtered_id)
    AND type_id = 'F8EM')
    SELECT region,
           activity_time,
           COUNT(*) AS total_ads,
           ROUND(AVG(metr_price)::numeric,2) AS avg_metr_price,
           ROUND(AVG(total_area)::numeric,2) AS avg_total_area,
           PERCENTILE_DISC (0.5) WITHIN GROUP (ORDER BY rooms) AS med_rooms,
           PERCENTILE_DISC (0.5) WITHIN GROUP (ORDER BY balcony) AS med_balcony,
           PERCENTILE_DISC (0.5) WITHIN GROUP (ORDER BY floor) AS med_floors
    FROM category
    GROUP BY region, activity_time
    ORDER BY region, total_ads DESC;
   
   --region         |activity_time  |total_ads|avg_metr_price|avg_total_area|med_rooms|med_balcony|med_floors
--------------------+---------------+---------+--------------+--------------+---------+-----------+----------
-----ЛенОбл         |до трех месяцев|      917|      67573.43|         50.88|        2|        1.0|         3
-----ЛенОбл         |больше полугода|      890|      68297.22|         55.41|        2|        1.0|         3
-----ЛенОбл         |до полугода    |      556|      69846.39|         51.83|        2|        1.0|         3
-----ЛенОбл         |прочие         |      461|      73625.63|         57.87|        2|        1.0|         3
-----ЛенОбл         |до месяца      |      397|      73275.25|         48.72|        2|        1.0|         4
-----Санкт-Петербург|больше полугода|     3581|     115457.22|         66.15|        2|        1.0|         5
-----Санкт-Петербург|до трех месяцев|     3236|     111573.24|         56.71|        2|        1.0|         5
-----Санкт-Петербург|до полугода    |     2254|     111938.92|         60.55|        2|        1.0|         5
-----Санкт-Петербург|до месяца      |     2168|     110568.88|         54.38|        2|        1.0|         5
-----Санкт-Петербург|прочие         |     1554|     134632.92|         72.03|        2|        2.0|         5
 
   
   
 --2.
 -- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS
    (SELECT  
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
          PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
          PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
 ),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS
    (SELECT id
    FROM real_estate.flats  
    WHERE total_area < (SELECT total_area_limit FROM limits)
          AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
          AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
          AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
 ),
tab AS
   (SELECT a.id,
    total_area,
    last_price/total_area AS price_kvm,
    EXTRACT ('month' FROM first_day_exposition::DATE) AS first_month,
    EXTRACT ('month' FROM first_day_exposition::DATE + days_exposition::INTEGER) AS arch_month
 FROM real_estate.flats AS f
 JOIN real_estate.advertisement AS a ON a.id=f.id
 WHERE f.id IN (SELECT id FROM filtered_id) AND type_id = 'F8EM'
    ),
publ_month AS -- статистика по месяцу публикации           
    (SELECT first_month, 
     COUNT(id) AS count_obv,
     ROUND(AVG(price_kvm)::numeric, 2) AS avg_price_kv, -- цена за кв.м.,
     ROUND(AVG(total_area)::numeric, 2) AS avg_ar
FROM tab
GROUP BY first_month
ORDER BY first_month
),
arch_month AS -- статистика по месяцу публикации           
     (SELECT arch_month, 
      COUNT (id) AS count_obv,
      ROUND (AVG(price_kvm)::numeric, 2) AS avg_price_kv, -- цена за кв.м.,
      ROUND (AVG(total_area)::numeric, 2) AS avg_ar
FROM tab
GROUP BY arch_month
ORDER BY arch_month
)
--посчитать общее количество объявлений и потом разделить количество объявлений в каждом месяце на общее количество.
SELECT 'опубликовано' AS type, 
        *,
        RANK() OVER (ORDER BY count_obv DESC) AS rank_month
        FROM publ_month
UNION ALL
SELECT 'снято' AS type, 
        *,
        RANK() OVER (ORDER BY count_obv DESC) AS rank_month
        FROM arch_month;
       
--type        |first_month|count_obv|avg_price_kv|avg_ar|rank_month|
--------------+-----------+---------+------------+------+----------+
--опубликовано|          2|     1737|   106452.02| 60.34|         1|
--опубликовано|          3|     1675|   107320.49| 59.41|         2|
--опубликовано|          4|     1638|   108450.83| 59.97|         3|
--опубликовано|         11|     1589|   105468.23| 60.03|         4|
--опубликовано|         10|     1437|   104065.11| 59.43|         5|
--опубликовано|          9|     1341|   107563.12| 61.04|         6|
--опубликовано|          6|     1224|   104802.15| 58.37|         7|
--опубликовано|          8|     1166|   107034.70| 58.99|         8|
--опубликовано|          7|     1149|   104488.96| 60.42|         9|
--опубликовано|         12|     1112|   106768.32| 60.89|        10|
--опубликовано|          1|     1017|   106835.92| 58.96|        11|
--опубликовано|          5|      929|   103510.69| 59.20|        12|
--снято       |           |     2015|   120675.42| 68.79|         1|
--снято       |          4|     1420|   105892.67| 58.23|         2|
--снято       |         10|     1367|   104608.70| 58.91|         3|
--снято       |         11|     1307|   103882.35| 56.87|         4|
--снято       |          3|     1276|   107554.43| 59.45|         5|
--снято       |          1|     1268|   105120.97| 57.67|         6|
--снято       |          9|     1247|   104397.12| 57.75|         7|
--снято       |         12|     1179|   105712.63| 59.39|         8|
--снято       |          8|     1145|   100056.84| 56.98|         9|
--снято       |          7|     1130|   102505.11| 59.04|        10|
--снято       |          2|     1128|   104032.82| 60.47|        11|
--снято       |          6|      782|   101912.78| 60.19|        12|
--снято       |          5|      750|   100356.52| 57.49|        13|       
       
       
       
--3. 
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS 
(
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
)
-- Найдём id объявлений, которые не содержат выбросы:
, filtered_id AS
(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    )
, archive AS 
(
    SELECT  
        city,
        COUNT (a.id) AS count_archive_id, --кол-во снятых обьявлений
        AVG(last_price/total_area) AS avg_price_kvm, --цена за метр снятых обьявлений 
        AVG(total_area) AS avg_total_area --общая площадь снятых обьявлений       
    FROM real_estate.advertisement AS a
        JOIN real_estate.flats AS f ON f.id=a.id
        JOIN real_estate.city AS c ON c.city_id=f.city_id
    WHERE 
        days_exposition IS NOT NULL
        AND f.id IN (SELECT * FROM filtered_id)
    GROUP BY city
),
total_id AS 
(
    SELECT  
        city,
        COUNT(a.id) AS total_count_id --общее кол-во обьявлений
    FROM real_estate.advertisement AS a
        JOIN real_estate.flats AS f ON f.id=a.id
        JOIN real_estate.city AS c ON c.city_id=f.city_id
    WHERE 
        f.id IN (SELECT * FROM filtered_id)
    GROUP BY city
)
SELECT 
    archive.city,
    total_count_id,
    archive.count_archive_id, 
    ROUND(count_archive_id/total_count_id::NUMERIC, 2) AS shear, --доля снятых обьявлений в общем количестве
    ROUND(avg_price_kvm::NUMERIC, 2),
    ROUND(avg_total_area::NUMERIC, 2)
FROM archive
    JOIN total_id ON total_id.city=archive.city
WHERE 
    archive.city <> 'Санкт-Петербург' 
    AND total_count_id > 50
ORDER BY total_count_id DESC;


--city           |total_count_id|count_archive_id|shear|round    |round|
-----------------+--------------+----------------+-----+---------+-----+
--Мурино         |           568|             532| 0.94| 85655.03|43.64|
--Кудрово        |           463|             434| 0.94| 94583.20|45.87|
--Шушары         |           404|             374| 0.93| 78109.78|53.61|
--Всеволожск     |           356|             305| 0.86| 68796.13|54.43|
--Парголово      |           311|             288| 0.93| 89711.32|50.66|
--Пушкин         |           278|             231| 0.83|103238.60|58.06|
--Гатчина        |           228|             203| 0.89| 67977.76|50.47|
--Колпино        |           227|             209| 0.92| 74644.96|52.26|
--Выборг         |           192|             168| 0.88| 57683.59|54.21|
--Петергоф       |           154|             136| 0.88| 83992.17|50.26|
--Сестрорецк     |           149|             134| 0.90|103956.59|60.87|
--Красное Село   |           136|             122| 0.90| 71671.95|52.11|
--Новое Девяткино|           120|             106| 0.88| 76278.96|50.87|
--Сертолово      |           117|             101| 0.86| 68509.24|53.87|
--Бугры          |           104|              91| 0.88| 80118.07|46.74|
--Волхов         |            87|              68| 0.78| 34825.59|48.36|
--Ломоносов      |            87|              80| 0.92| 70959.16|50.75|
--Кингисепп      |            84|              77| 0.92| 46568.02|53.40|
--Никольское     |            80|              69| 0.86| 57114.54|42.82|
--Сланцы         |            79|              66| 0.84| 18328.07|48.35|
--Кронштадт      |            70|              63| 0.90| 79528.81|53.66|
--Коммунар       |            66|              56| 0.85| 57585.65|48.05|
--Янино-1        |            64|              55| 0.86| 68137.50|48.75|
--Старая         |            58|              50| 0.86| 64762.97|54.62|
--Тосно          |            58|              54| 0.93| 58987.24|53.35|
--Сосновый Бор   |            54|              47| 0.87| 74066.72|53.01|
--Отрадное       |            53|              40| 0.75| 56517.86|52.12|
    
    