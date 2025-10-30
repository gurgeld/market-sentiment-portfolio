
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select published_date
from "warehouse"."main_gold"."mart_daily_sentiment"
where published_date is null



  
  
      
    ) dbt_internal_test