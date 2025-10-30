
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select news_id
from "warehouse"."main_silver"."stg_ticker_sentiment"
where news_id is null



  
  
      
    ) dbt_internal_test