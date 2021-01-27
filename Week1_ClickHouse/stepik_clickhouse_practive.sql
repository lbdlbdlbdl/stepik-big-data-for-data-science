select * from ads_data limit 50;

-- В этом задании вычислите количество партиций в таблице ads_data.
select count(DISTINCT partition) from system.parts where table='ads_data';

/*
Объявления из таблицы ads_data показываются людям на разных платформах.
Вычислите, сколько всего просмотров было совершенно с платформы Android и iOS?
 */
select count(event) from ads_data
where 0=0
and platform='android'
-- and platform='ios'
and event='view';

-- с помощью countIf:
select countIf(platform='android' and event='view') from ads_data;

/*
 Объявления из таблицы ads_data показываются людям на разных платформах.
 Вычислите, с какого максимального количества платформ может быть просмотрено одно объявление?
 */
 select uniqExact(platform) from ads_data;

/*
 Заполнить пропуски.
 */
with (select uniqExact(ad_id) from ads_data) as sum_ads

select platforms, uniqExact(ad_id) as ads, sum_ads, ads / sum_ads * 100 as perc_ads
from (select ad_id, arrayDistinct(groupArray(platform)) as platforms
      from ads_data
      where event = 'view'
      group by ad_id)
group by platforms
order by perc_ads desc;

/*
 В таблице ads_clients_data хранятся данные обо всех клиентах, которые существовали на платформе 7 мая 2020 года.
 Подобные логи называют дампами. Они показывают как бы слепок системы в каком-то разрезе за определенный день.
 В данном случае это слепок всех клиентов рекламного кабинета за конкретный день.

Мы знаем,  что не все кабинеты запускают рекламу на нашей платформе.
 Некоторые из них вообще не имеют когда-либо запущенных или активных рекламных объявлений.

В этом задании, исключите из таблицы ads_clients_data всех клиентов, у которых когда-либо были активные объявления (для этого понадобится таблица ads_data) и заполните пропуски ниже.

Hint: Таблица ads_data - это таблица, в которой по определению есть только объявления, которые уже были запущены (которые активны)

Задание. Для того чтобы исключить всех клиентов, у которых когда-либо были активные объявления на нашей площадке, я воспользуюсь выражением
? (GLOBAL NOT IN/ NOT IN). В результате остается ? (121240) клиентов, которые никогда не запускали рекламу в нашем кабинете.
Такие клиенты составляют ? (99) процентов от общего количества клиентов на 7 мая 2020 года (ответ округлен до целого).
Самые старшие клиенты были созданы (данные о дне создания клиента лежат в колонке create_date) ? (2018-07-28) (данные в этом пропуске должны быть в формате yyyy-mm-dd).
 */
select count(*) from ads_clients_data limit 50;

select
       count(acd.client_union_id) as not_active,
       round(not_active * 100 / (select count(client_union_id) from ads_clients_data)),
       -- todo посмотреь что быстрее
       arraySort(array(create_date)),
       (select create_date from ads_clients_data order by create_date limit 1) as create_date
from ads_clients_data acd
where 0=0
and client_union_id global not in
    (select client_union_id as active from ads_data ad);

-- другой вариант
with (select uniqExact(client_union_id) from ads_clients_data) as total
select count(client_union_id) as never, min(create_date) as oldest, total, round(never / total * 100) as part from ads_clients_data
where client_union_id not in (select distinct client_union_id from ads_data);

-- другой вариант
with (select uniqExact(client_union_id)
from ads_clients_data
where client_union_id GLOBAL NOT IN (select distinct client_union_id from ads_data)) as clients_7_may_without_ads

select uniqExact(client_union_id) as all_clients, clients_7_may_without_ads,
       round(clients_7_may_without_ads/all_clients*100) as percentage,
       min(create_date) as earliest_client
from ads_clients_data;

/*
Определите среднее, минимальное и максимальное количество дней, которые проходят с момента создания клиента (колонка create_date в таблице ads_clients_data) до момента запуска первого объявления этим клиентом (таблица ads_data).
 */

--select min(diff), round(avg(diff)), max(diff)
--       from (
select
       acd.create_date created,
       abs(datediff('day', ad.time, created)) diff
       --abs(min(datediff('day', ad.time, created))) min,
       --abs(avg(datediff('day', ad.time,created))) avg
from ads_clients_data acd
inner join ads_data ad on acd.client_union_id = ad.client_union_id;
--);

select min(diff), round(avg(diff)), max(diff)
      from (
select
       client_union_id,
       acd.create_date,
      arraySort(groupArray(ad.time)) times,
       abs(datediff('day', arrayElement(times,1), acd.create_date)) diff
from ads_clients_data acd
inner join ads_data ad on acd.client_union_id = ad.client_union_id
group by client_union_id, create_date
order by client_union_id);
--acd.create_date, ad.time;

-- todo добавить другие способы

