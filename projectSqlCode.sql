-- czechia_price
-- czechia_region
-- czechia_price_category
-- czechia_payroll
-- czechia_payroll_industry_branch
--czechia_payroll_unit
--czechia_payroll_calculation

---------------------------
-- main table one
---------------------------

create table t_Adam_Lizal_project_SQL_primary_final; as (
select 
	  cp.value as price_value,
	  cast(cp.date_from as date),
	  cast(cp.date_to as date),
	  cpc.name as food_category,
	  cpc.price_value as food_category_value,
	  cpc.price_unit as food_category_unit,
	  cr.name as region_name,
	  cpay.value as payroll_value,
	  cpib.name as name_of_industry
from czechia_price as cp
left join czechia_price_category as cpc 
on cpc.code = cp.category_code
left join czechia_region as cr
on cr.code = cp.region_code
left join czechia_payroll as cpay
on extract(year from cp.date_from) = cpay.payroll_year
left join czechia_payroll_industry_branch as cpib
on cpib.code = cpay.industry_branch_code 
where cpay.value_type_code = 5958
);

create index idx_primary_final on t_Adam_Lizal_project_SQL_primary_final(name_of_industry);
create index idx_1_primary_final on t_Adam_Lizal_project_SQL_primary_final(payroll_value);
create index idx_2_primary_final on t_Adam_Lizal_project_SQL_primary_final(date_from);
create index idx_3_primary_final on t_Adam_Lizal_project_SQL_primary_final(date_to,year_primary);



-----------------
--main table two
-----------------

create table t_Adam_Lizal_project_SQL_secondary_final as (
select
	  e.year,
	  c.country,
	  e.population,
	  e.gdp,
	  e.gini
from countries as c
join economies as e
on c.country = e.country
where continent = 'Europe'
and e.year between 2006 and 2018
order by year, country
);

CREATE INDEX idx_secondary_year ON t_adam_lizal_project_sql_secondary_final (year);

---------------------------
-- 1.Do wages increase across all sectors over the years, or do they decline in some?
---------------------------
create view test_table as
select 
	 name_of_industry,
	 year_primary, --extract(year from date_from) as year,
	 round(avg(payroll_value::decimal),2) as avg_payroll,
	 lag(round(avg(payroll_value::decimal),2))over(partition by name_of_industry order by year_primary) as prev_year_avg_payroll,
	 round(avg(payroll_value::decimal),2) - lag(round(avg(payroll_value::decimal),2))over(partition by name_of_industry order by year_primary) as difference
from t_adam_lizal_project_sql_primary_final talpspf
group by name_of_industry, year_primary
order by name_of_industry, year_primary 
;

select * from test_table tt ;

		--------------------
		-- I would like to see in which sectors and when, the wages declined
		--------------------
		select * from test_table
		where difference < 0
		;
		
		--------------------
		-- I would like to see difference percentage between payroll 2006 and 2018
		--------------------
		
		select
		    tt.name_of_industry,
		    round((SUM(tt.difference)*100 / y2018.avg_payroll_2018 ),2) as percentage_grow
		from test_table tt
		left join (
		    select
		        name_of_industry,
		        avg_payroll AS avg_payroll_2018
		    from test_table
		    where year_primary = 2006
		) as y2018
		    on tt.name_of_industry = y2018.name_of_industry
		group by tt.name_of_industry, y2018.avg_payroll_2018
		;


		
--------------------------
-- 2. How much liter of milk and Kilogram of bread is possible to buy on first and last year in observated period 
--------------------------		

select 
	 food_category,
	 round(avg(price_value::decimal),2) as avg_price,
	 round(avg(payroll_value::decimal),2) as avg_payroll,
	 year_primary,
	 round(round(avg(payroll_value::decimal),2)/round(avg(price_value::decimal),2)) as buy_power,
	 row_number()over(order by food_category) as help_value
from t_adam_lizal_project_sql_primary_final talpspf
where extract(year from date_from) in (2006, 2018)
and
food_category in ('Chléb konzumní kmínový','Mléko polotučné pasterované' )
group by year_primary, food_category
order by food_category;



select distinct year_primary from t_adam_lizal_project_sql_primary_final talpspf ;

----------------------------------------------------------------
-- 3. Which category of food is becoming more expensive at the slowest rate (i.e., has the lowest percentage year-over-year increase)?
----------------------------------------------------------------


with cte_base as (
select
	  extract(year from date_from) as year,
	  food_category,
	  round(avg(price_value::decimal),2) as avg_price,
	  lag(round(avg(price_value::decimal),2))over(partition by food_category order by extract(year from date_from)) as prev_price,
	  round(avg(price_value::decimal),2) - lag(round(avg(price_value::decimal),2))over(partition by food_category order by extract(year from date_from))as year_difference
from t_adam_lizal_project_sql_primary_final 
group by food_category, extract(year from date_from)
 ),
cte_diff as (
select
	 food_category,
	 year,
	 round((year_difference/prev_price)*100,2) as percentage_diff
from cte_base
where prev_price is not null and year_difference is not null
and year_difference >=0 
 ),
 cte_rank as (
 select 
 	  food_category,
 	  year,
 	  percentage_diff,
 	  row_number()over(order by percentage_diff asc) as r_min,
 	  row_number()over(order by percentage_diff desc) as r_max
 from cte_diff
 )
 select 
       food_category,
       year,
       percentage_diff,
       'min' as result
 from cte_rank
 where r_min = 1
 union all
  select 
       food_category,
       year,
       percentage_diff,
       'max' as result
 from cte_rank
 where r_max = 1
;


--------------------------------------
-- 4. Is there a year in which the year-over-year increase in food prices was significantly higher than the growth in wages (by more than 10%)?
-------------------------------------

with cte_main as (
select 
		tt.name_of_industry, 
		round((difference/prev_year_avg_payroll)*100,2)as percentage_diff_payroll,
		price.prev_price,
		price.year_difference,
		tt.year_primary
from test_table as tt
inner join (
		select
			 name_of_industry,
			 extract(year from date_from) as year,
			 round(avg(price_value::decimal),2) as avg_price,
	  		 lag(round(avg(price_value::decimal),2))over(partition by name_of_industry order by extract(year from date_from)) as prev_price,
	         round(avg(price_value::decimal),2) - lag(round(avg(price_value::decimal),2))over(partition by name_of_industry order by extract(year from date_from))as year_difference
	     from t_adam_lizal_project_sql_primary_final talpspf
	     group by name_of_industry, extract(year from date_from)	 
) as price on tt.name_of_industry = price.name_of_industry and tt.year_primary = price.year
),
cte_main_filtered as(
select 
		 cte_main.year_primary,
		 name_of_industry,
		 cte_main.percentage_diff_payroll,
		 cte_main.prev_price,
		 cte_main.year_difference,
		 round((year_difference/prev_price)*100,2) as percentage_diff_price
from cte_main
where percentage_diff_payroll > 0 
and percentage_diff_payroll is not null
and round((year_difference/prev_price)*100,2) > 0
AND prev_price IS NOT NULL
AND year_difference IS NOT NULL
)
select
	 year_primary,
	 name_of_industry,
	 percentage_diff_payroll,
	 percentage_diff_price,
	 (percentage_diff_price - percentage_diff_payroll) as final_diff
from cte_main_filtered
order by (percentage_diff_price - percentage_diff_payroll) desc
;

------------------------------------------------
-- 5. Does the level of GDP have an impact on changes in wages and food prices?
------------------------------------------------

create view gdp_analysis as
with pf_prepered as (
select
	year_primary,
	price_value,
	payroll_value
from t_adam_lizal_project_sql_primary_final
),
sf_prepered as (
select
	sf.year,
	sf.country,
	round(avg(sf.gdp::decimal),2) as avg_gdp,
	round(avg(sf.gini::decimal),2) as avg_gini,
	round(avg(pf.price_value::decimal),2) as avg_price_value,
	round(avg(pf.payroll_value::decimal),2) as avg_payroll
from t_adam_lizal_project_sql_secondary_final as sf
left join pf_prepered as pf on sf.year = pf.year_primary
where country = 'Czech Republic'
and sf.year between 2006 and 2018
group by sf.year, sf.country
order by sf.year
),
sf_filtered_gdp as (
select
	 sfg.year,
	 avg_gdp,
	 lag(avg_gdp)over(order by year) as prev_gdp,
	 avg_gdp - lag(avg_gdp)over(order by year) as diff_gdp,
	 avg_price_value,
	 lag(avg_price_value)over(order by year) as prev_price_value,
	 avg_price_value - lag(avg_price_value)over(order by year) as diff_price,
	 avg_payroll,
	 lag(avg_payroll)over(order by year) as prev_payroll,
	 avg_payroll - lag(avg_payroll)over(order by year) as diff_payroll
from sf_prepered as sfg
),
sf_filtered_diff as (
select
	 sfd.year,
	 round((diff_gdp/prev_gdp)*100,2) as gdp_diff_perc,
	 round((diff_price/prev_price_value)*100,2) as price_diff_perc,
	 round((diff_payroll/prev_payroll)*100,2) as payroll_diff_perc 
from sf_filtered_gdp as sfd
)
select * from sf_filtered_diff 
;

select * from gdp_analysis;

		--------------------------------------------
		-- Only gdp growth
		--------------------------------------------
		
		select
			 year,
			 gdp_diff_perc,
			 price_diff_perc,
			 payroll_diff_perc
		from gdp_analysis
		where gdp_diff_perc > 0 
		and gdp_diff_perc is not null;

		----------------------------------------
		-- correlation between gdp vs price and payroll
		----------------------------------------
		SELECT
		  corr(gdp_diff_perc, payroll_diff_perc) AS corr_gdp_vs_payroll,
		  corr(gdp_diff_perc, price_diff_perc) AS corr_gdp_vs_price
		FROM gdp_analysis;
