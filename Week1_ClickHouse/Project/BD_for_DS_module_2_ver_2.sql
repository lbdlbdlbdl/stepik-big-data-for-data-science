select *
from ads_data
limit 50;

/* 1.
 Получить статистику по дням. Просто посчитать
 число всех событий по дням, число показов, число кликов, число уникальных объявлений и уникальных кампаний.
 */
select date,
       count(event)                      as all_events,
       countIf(event = 'view')           as views,
       countIf(event = 'click')          as clicks,
       count(distinct ad_id)             as unique_ads,
       count(distinct campaign_union_id) as unique_campaigns
from ads_data
group by date;

/* 2.
   Разобраться, почему случился такой скачок 2019-04-05? Каких событий стало больше? У всех объявлений или только у некоторых?
 */

-- версия 1 к мягкому дедлайну
/*
Запрос выполняется для всех объявлений, которые были показаны в том числе 2019_04_05го.

Если посмотреть результат самого внутреннего запроса (строка 42), можно увидеть, что выброс числа ивентов 2019_04_05го был не для всех объявлений.
В результате выполнения всего запроса поле perc_when_max_views_was_on_2019_04_05 показывает, что выброс на эту дату пришелся только для 46.8%.
(Выброс -- максимальное число показа объявлений пришлось на этот день.)
То есть скачок был не у всех объявлений.

Почему случился скачок? Появилась гипотеза, что скачок появляется при первом показе объявления.
В поле perc_when_2019_04_05_was_the_first_day_of_view результата мы видим, что процент объявлений, которые появились 2019_04_05го в первый раз, составил 46.0%.
В запросе для поля is_first_and_most_eventable дополнительная проверка: (по данному объявлению в этот день был выброс И в этот день оно было впервые показано), в процентах относительно общего числа объявлений такие составили 46%.

Разница процента выбросов в этот день и процента первых показов в день составляет меньше 1%. Тогда можно сделать вывод, что такой скачок произошел из-за впервые показанных объявлений.
 */
/*
-- внешний select для проверки главных гипотез
select count(ad_id)                                           as count,                                          -- общее число объявлений, которые были показаны в том числе 2019_04_05 (проверка на это во вложенном запросе)
      sum(are_max_ad_events_was_in_2019_04_05) * 100 / count as perc_when_max_views_was_on_2019_04_05,          -- Сколько процентов максимального числа событий в день  произошло 2019_04_05 в сравнении с другими днями. Скачок в этот день был не у всех объявлений.
      sum(is_2019_04_05_was_first_ad_day) * 100 / count      as perc_when_2019_04_05_was_the_first_day_of_view, -- какой процент объявлений был впервые показан 2019_04_05

      countIf(are_max_ad_events_was_in_2019_04_05 = 1 and is_2019_04_05_was_first_ad_day = 1) * 100 /
      count                                                  as is_first_and_most_eventable
from (
     select ad_id,
            arraySort(groupArray(toYYYYMMDD(date)))                            as dates,                               -- используется как переменная, в результирующей выборке не нужно. todo можно как-то вынести?
            arrayMap((d, e) -> (d, e),
                     dates,
                     groupArray(all_events))                                   as date_events_count,
            indexOf(dates, 20190405)                                           as indexOf_2019_04_05,                  -- т.к. в версии 19.13 indexOf не поддерживается, воспользовалась костылем toYYYYMMDD() двумя строками выше
            arrayReduce('max', date_events_count.2)                            as max_events_in_day,

            equals(date_events_count[indexOf_2019_04_05].2, max_events_in_day) as are_max_ad_events_was_in_2019_04_05, -- произошло ли 2019-04-05 максимальное колво событий по объявлению в сравнении с предыдущими днями
            equals(indexOf_2019_04_05, 1)                                      as is_2019_04_05_was_first_ad_day
     from (
           select ad_id,
                  date,
                  count(event) as all_events
           from ads_data
           where ad_id global in (select ad_id from ads_data where date = '2019-04-05')
           group by ad_id, date
           order by ad_id, date
          )
     group by ad_id)
limit 350;

   /*
-- Каких событий стало больше?
-- В сравнении с предыдщим днем стало больше кликов и показов.
-- todo проверить по каждому объявлению

select date,
       count(event)             as all_events,
       countIf(event = 'view')  as views,
       countIf(event = 'click') as clicks
from ads_data
group by date
order by date;

 */

-- версия 2 для жесткого дедлайна
/* смотрим, для каких объявлений 2019-04-05го было больше всего событий. на первом месте по кол-ву событий объявление 112583.
 */
select ad_id,
       count(event) as all_events_count
from ads_data
where ad_id global in (select ad_id from ads_data where date = '2019-04-05')
group by ad_id
order by all_events_count desc;

-- используем предыдущий запрос как подзапрос в следующем запросе, чтобы посмотреть динамику роста событий в рамках объявления по дням.
-- сравниваем полученные результаты, и видим, что все объявления кроме 112583 имели своим пиком кол-во событий дату, отличную 2019-04-05 (в предыдущий день событий было больше).
-- т.е. причиной выброса было размещение объявления 112583
select ad_id,
       date,
       count(event)             as all_events,
       countIf(event = 'view')  as views,
       countIf(event = 'click') as clicks
from ads_data
where ad_id global in (
    select ad_id
    from (select ad_id,
                 count(event) as all_events_count
          from ads_data
          where ad_id global in (select ad_id from ads_data where date = '2019-04-05')
          group by ad_id
          order by all_events_count desc
          limit 5))
group by ad_id, date
order by ad_id desc, date;

/* 3.
 Найти топ 10 объявлений по CTR за все время. CTR — это отношение всех кликов объявлений к просмотрам.
 Например, если у объявления было 100 показов и 2 клика, CTR = 0.02. Различается ли средний и медианный CTR объявлений в наших данных?
 */
-- Найти топ 10 объявлений по CTR за все время.
select ad_id,
       countIf(event = 'view')  as views,
       countIf(event = 'click') as clicks,
       clicks / views           as ctr
from ads_data
group by ad_id
having views != 0 -- баг в данных
order by ctr desc
limit 10;
-- Различается ли средний и медианный CTR объявлений в наших данных?
select medianExact(ctr), avg(ctr)
from (select countIf(event = 'view')  as views,
             countIf(event = 'click') as clicks,
             clicks / views           as ctr
      from ads_data
      group by ad_id
      having views != 0);

/* 4.
 Похоже, в наших логах есть баг, объявления приходят с кликами, но без показов!
 Сколько таких объявлений, есть ли какие-то закономерности? Эта проблема наблюдается на всех платформах?
 */
-- сколько таких объявлений?
select count(ad_id)
from (
      select ad_id,
             countIf(event = 'click') as clicks,
             countIf(event = 'view')  as views
      from ads_data
      group by ad_id
      having views = 0
         and clicks != 0
      order by views);

/*
 Чтобы найти закономерности (начиная со строки 131), посмотрим в таблице ads_data  информацию по всем объявлениям у которых число просмотров = 0
при ненулевом кол-ве кликов. Поля которые нам ни о чем не скажут, не рассматриваем.

Что можно заметить:
 - Все такие объявления были кликнуты (но не просмотрены) 2019-04-01.
 - большая часть таких объявлений были кликнуты на мобильных платформах. android - 47, ios - 30, web - 12
 */

select countIf(platform = 'android') as androids,
       countIf(platform = 'ios')     as ios,
       countIf(platform = 'web')     as web
from (
      select ad_id,
             time,
             platform
      from ads_data
      where ad_id in (
          select ad_id
          from (
                select ad_id,
                       countIf(event = 'click') as clicks,
                       countIf(event = 'view')  as views
                from ads_data
                group by ad_id
                order by views)
          where views = 0
            and clicks != 0
      )
      order by time, platform
      limit 100);


/* 5.
 Есть ли различия в CTR у объявлений с видео и без? А чему равняется 95 процентиль CTR по всем объявлениям за 2019-04-04?
 */
/* todo
with ( -- учитываем колво просмотров кликнутых объявлений которые не были посчитаны
    select count(ad_id)
    from (
          select ad_id,
                 countIf(event = 'click') as clicks,
                 countIf(event = 'view')  as views
          from ads_data
          group by ad_id
          order by views)
    where views = 0
      and clicks != 0) as nullViewsCount
 */

-- Есть ли различия в CTR у объявлений с видео и без?
select has_video,
       countIf(event = 'view')  as views,
       countIf(event = 'click') as clicks,
       clicks / views           as ctr
from ads_data
group by has_video
having views != 0;

-- А чему равняется 95 процентиль CTR по всем объявлениям за 2019-04-04?
select quantileExact(0.95)(ctr)
from (select ad_id,
             countIf(event = 'view')  as views,
             countIf(event = 'click') as clicks,
             clicks / views           as ctr
      from ads_data
      where date = '2019-04-04'
      group by ad_id
      having views != 0);


/* 6.
Для финансового отчета нужно рассчитать наш заработок по дням. В какой день мы заработали больше всего? В какой меньше?
Мы списываем с клиентов деньги, если произошел клик по CPC объявлению, и мы списываем деньги за каждый показ CPM объявления, если у CPM объявления цена - 200 рублей, то за один показ мы зарабатываем 200 / 1000.
 */
-- больше всего заработали в 2019-04-05, меньше всего в 2019-04-01
select date,
       sum(if(ad_cost_type = 'CPC' and event = 'click', ad_cost, 0))       as CPC,
       sum(if(ad_cost_type = 'CPM' and event = 'view', ad_cost / 1000, 0)) as CPM,
       CPC + CPM                                                           as sum
from ads_data
group by date
order by sum desc;

/* 7.
 На какой платформе больше всего показов? Сколько процентов показов приходится на каждую из платформ (колонка platform)?
 */
-- на платформе android. Проценты: android - 50, ios - ~30, web - ~20
with (select countIf(event = 'view') from ads_data) as total_views_count

select platform,
       countIf(event = 'view')                  as platform_views,
       platform_views * 100 / total_views_count as perc
from ads_data
group by platform
order by platform_views desc;

/* 8.
 А есть ли такие объявления, по которым сначала произошел клик, а только потом показ?
 */
/* версия 1 к мягкому дедлайну
-- будем считать, что в рамках одного объявления последовательность показ-клик возможна в течение одного часа (если клик произошел через час после показа, это не считается).
-- (по-хорошему надо проверять хотя бы среднее время которое пользователь проводит на странице)
select sum(clickMoreThanHour)
from (select time,
             ad_id,
             event,
             if(event = 'click', 1, 0) as eventCast,
             if(runningDifference(time) > 3600 and runningDifference(eventCast), 1,
                0)                     as clickMoreThanHour -- runningDifference(DateTime) в секундах
      from (
            select time,
                   ad_id,
                   event
            from ads_data
            order by ad_id, time));
*/

/* версия 2 к жесткому дедлайну */
SELECT uniq(ad_id)
FROM (SELECT ad_id, platform, argMin(event, time) as first_event
      FROM ads_data
      GROUP BY ad_id, platform
      HAVING first_event = 'click');
